[<AutoOpen>]
module Kafka.Transition

open System
open FSharpPlus
open FSharpPlus.Data
open FSharpPlus.Lens
open Microsoft.FSharp.Core
open Types
open Kafka

let logIndex (key: LogKey) (offset: Offset) : string =
    $"{key.Value}-{offset.Value}"

let genMessageId (node: Node) : Node * MessageId =
    {
        node with
            NextMessageId = node.NextMessageId + 1
    },
    MessageId node.NextMessageId


let refreshLog (logKey: LogKey) node (f : Node -> TransitionResult) : TransitionResult =
    let rec refreshLogsNext (node: Node) : TransitionResult =
        let node, queryMessageId = genMessageId node
        let nextOffset =
            match node.CachedMessages.TryFind logKey with
            | None -> Offset 0
            | Some messages ->
                    Offset messages.Count
        let seqKVReadLogMessageBody: OutputMessageBody =
            KVRequest (KVRequestMessageBody.Read (queryMessageId, logIndex logKey nextOffset))
        let seqKVReadLogMessage =
            {
                Source = node.Info.NodeId
                Destination = NodeId.SeqKv
                MessageBody = seqKVReadLogMessageBody
            }
        let node = node.RegisterReadOkHandler queryMessageId (fun node (Value value) ->
            let updatedLogs =
                node.CachedMessages.TryFind logKey
                |> Option.defaultValue Map.empty
                |> Map.add nextOffset (LogValue value)

            let node = { node with CachedMessages = node.CachedMessages.Add(logKey, updatedLogs) }
            refreshLogsNext node
        )
        let node = node.RegisterErrorKeyDoesNotExistHandler queryMessageId (fun node -> f node)
        node, [seqKVReadLogMessage]
    refreshLogsNext node

let rec refreshLogs (logKeys: List<LogKey>) node (f : Node -> TransitionResult) : TransitionResult =
    match logKeys with
    | [] -> f node
    | x :: xs ->
        refreshLog x node (fun node -> refreshLogs xs node f)

let assertConsistency (node: Node) : unit =
    node.CachedMessages
    |> Map.forall (fun logKey values ->
        values.Keys
        |> Seq.toList
        |> fun offsets ->
            offsets = List.init offsets.Length (fun i -> Offset i)
    )
    |> fun x ->
        eprintfn "assertConsistency: %A" x
        if x = false then
            failwithf "assertConsistency failed"

let transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : TransitionResult =
    assertConsistency node
    match action with
    | Choice2Of2 unit ->
        (node, List.empty)
    | Choice1Of2 msg ->
        match msg.MessageBody with
        | InputMessageBody.Send(messageId, key, value) ->
            // read current offset from lin-kv
            // increment current offsets from lin-kv
            // write log value to seq-kv
            let replySendOk (node: Node) (offset: Offset) : TransitionResult =
                let sendOkReplyMessageBody =
                    OutputMessageBody.SendAck (messageId, offset)
                let sendOkReplyMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = sendOkReplyMessageBody
                    }
                eprintfn $"send: key: %A{key} value: %A{value} send_ok reply on {offset}"
                node, [sendOkReplyMessage]

            let withWriteLog (node: Node) (offset: Offset) : TransitionResult =
                let node, writeLogMessageId = genMessageId node
                let seqKVWriteMessageBody: OutputMessageBody =
                    KVRequest (KVRequestMessageBody.Write (writeLogMessageId, logIndex key offset, Value value.Value))
                let seqKVWriteMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = NodeId.SeqKv
                        MessageBody = seqKVWriteMessageBody
                    }
                let node = node.RegisterWriteOkHandler writeLogMessageId (fun node -> replySendOk node offset)
                eprintfn $"send: key: %A{key} value: %A{value} seq-kv write log in {offset} logIndex: {logIndex key offset}"
                node, [seqKVWriteMessage]

            let withLatestOffsetRead (key: LogKey) node (f: Node -> Offset -> TransitionResult) : TransitionResult =
                let node, queryMessageId = genMessageId node
                let linKVReadMessageBody: OutputMessageBody =
                    KVRequest (KVRequestMessageBody.Read (queryMessageId, $"current_offset_{key.Value}"))
                let linKVReadMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = NodeId.LinKv
                        MessageBody = linKVReadMessageBody
                    }

                let node = node.RegisterReadOkHandler queryMessageId (fun node (Value value) -> f node (Offset value))
                let node = node.RegisterErrorKeyDoesNotExistHandler queryMessageId (fun node -> f node (Offset -1))
                eprintfn $"send: key: %A{key} value: %A{value} lin-kv read offset"
                node, [linKVReadMessage]

            let rec withIncrementOffsetWrite (node: Node) (Offset offset) : TransitionResult =
                let nextOffset = Offset (offset + 1)
                let node, updateMessageId = genMessageId node
                let linKVWriteMessageBody: OutputMessageBody =
                    KVRequest (KVRequestMessageBody.CompareAndSwap (updateMessageId, $"current_offset_{key.Value}", Value offset, Value nextOffset.Value, nextOffset.Value = 0))
                let linKVWriteMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = NodeId.LinKv
                        MessageBody = linKVWriteMessageBody
                    }
                let node = node.RegisterCompareAndSwapOkHandler updateMessageId (fun node -> withWriteLog node nextOffset)
                let node = node.RegisterErrorPreconditionFailedHandler updateMessageId (fun node -> withLatestOffsetRead key node withIncrementOffsetWrite)
                eprintfn $"send: key: %A{key} value: %A{value} cas write to {offset + 1}"
                node, [linKVWriteMessage]

            withLatestOffsetRead key node withIncrementOffsetWrite

        | InputMessageBody.Poll (messageId, offsets) ->

            let onRefreshLogsCompleted node : TransitionResult =
                let messages: Map<LogKey, List<Offset * LogValue>> =
                    offsets
                    |> Map.choosei (fun key offset ->
                        node.CachedMessages.TryFind key
                        |> Option.map (Map.filter (fun offset' _ -> offset <= offset') >> Map.toList)
                    )
                messages
                |> Map.iter (fun key messages ->

                    node.CachedMessages.TryFind key
                    |> Option.map (eprintfn "key:%A: %A" key)
                    |> Option.defaultValue ()

                    let uniq = messages |> List.map fst |> List.distinct
                    if uniq.Length <> messages.Length then
                        eprintfn "key:%A: mesages %A" key messages
                        eprintfn "key:%A: uniq %A" key uniq
                        failwith "assertion failed"
                    assert (uniq.Length = messages.Length)
                )
                let replyMessageBody: OutputMessageBody =
                    PollAck (messageId, messages)
                let replyMessage: Message<OutputMessageBody> =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = replyMessageBody
                    }
                (node, [ replyMessage ])
            refreshLogs (offsets.Keys |> List.ofSeq) node onRefreshLogsCompleted
        | InputMessageBody.CommitOffsets(messageId, offsets) ->

            let writeMessages, node =
                offsets
                |> Map.toList
                |> List.mapFold (fun node (key, Offset offset) ->
                    let node, updateMessageId = genMessageId node
                    let linKVWriteMessageBody: OutputMessageBody =
                        KVRequest (KVRequestMessageBody.Write (updateMessageId, "committed_offset_" + key.Value, Value offset))
                    let linKVWriteMessage =
                        {
                            Source = node.Info.NodeId
                            Destination = NodeId.LinKv
                            MessageBody = linKVWriteMessageBody
                        }
                    let node = node.RegisterWriteOkHandler updateMessageId (fun node -> node, [])
                    linKVWriteMessage, node
                ) node

            let replyMessageBody: OutputMessageBody =
                CommitOffsetsAck (messageId)
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            (node, replyMessage :: writeMessages)

        | InputMessageBody.ListCommittedOffsets(messageId, keys) ->

            let queryMessages, node =
                keys
                |> NonEmptyList.toList
                |> List.mapFold (fun node logKey ->
                    let node, messageId = genMessageId node
                    let queryOffsetMessageBody =
                        KVRequest (KVRequestMessageBody.Read (messageId, "committed_offset_" + logKey.Value))
                    let queryOffsetMessage : Message<OutputMessageBody> =
                        {
                            Source = node.Info.NodeId
                            Destination = NodeId.LinKv
                            MessageBody = queryOffsetMessageBody
                        }
                    ((logKey, messageId), queryOffsetMessage), node
                ) node

            let replyListCommittedOffsets (offsets: Map<LogKey, Offset>) (node: Node) =
                let replyMessageBody: OutputMessageBody =
                    ListCommittedOffsetsAck (messageId, offsets)
                let replyMessage: Message<OutputMessageBody> =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = replyMessageBody
                    }
                node, [ replyMessage ]

            let rec responseMessageHandler (awaitingResponses: List<LogKey * MessageId>) (offsets: Map<LogKey, Offset>) (logKey, maybeOffset: Option<Offset>) (node: Node) : TransitionResult =
                let awaitingResponses = List.filter (fun x -> fst x <> logKey) awaitingResponses
                let offsets =
                    maybeOffset
                    |> Option.map (fun offset -> offsets.Add (logKey, offset))
                    |> Option.defaultValue offsets
                if awaitingResponses = [] then
                    replyListCommittedOffsets offsets node
                else
                    let node = registerAllCallbacks awaitingResponses offsets node
                    node, []
            and registerAllCallbacks (awaitingResponses: List<LogKey * MessageId>) (offsets: Map<LogKey, Offset>) (node: Node) : Node =
                awaitingResponses
                |> List.fold (fun (node: Node) (logKey, messageId) ->
                    let node = node.RegisterReadOkHandler messageId (fun node (Value value) -> responseMessageHandler awaitingResponses offsets (logKey, Some <| Offset value) node)
                    let node = node.RegisterErrorKeyDoesNotExistHandler messageId (fun node -> responseMessageHandler awaitingResponses offsets (logKey, None) node)
                    node
                ) node

            let awaitingResponses =
                queryMessages
                |> List.map fst

            let node =
                registerAllCallbacks awaitingResponses Map.empty node

            node, queryMessages |> List.map snd

        | InputMessageBody.KVResponse response ->
            match response with
            | KVResponseMessageBody.ReadOk(inReplyTo, value) ->
                node.OnKVReadOkHandlers.TryFind inReplyTo
                |> Option.get
                |> fun f -> f (node.UnregisterAllHandler inReplyTo) value
            | KVResponseMessageBody.WriteOk inReplyTo ->
                node.OnKVWriteOkHandlers.TryFind inReplyTo
                |> Option.get
                <| node.UnregisterAllHandler inReplyTo
            | KVResponseMessageBody.ErrorKeyDoesNotExist inReplyTo ->
                node.OnKVErrorKeyDoesNotExistHandlers.TryFind inReplyTo
                |> Option.get
                <| node.UnregisterAllHandler inReplyTo
            | KVResponseMessageBody.CompareAndSwapOk inReplyTo ->
                node.OnKVCompareAndSwapOkHandlers.TryFind inReplyTo
                |> Option.get
                <| node.UnregisterAllHandler inReplyTo
            | KVResponseMessageBody.ErrorPreconditionFailed inReplyTo ->
                node.OnKVErrorPreconditionFailedHandlers.TryFind inReplyTo
                |> Option.get
                <| node.UnregisterAllHandler inReplyTo

let transitionOuter (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : TransitionResult =

    // node.OnKVWriteOkHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "write ok keys: %A"
    // node.OnKVCompareAndSwapOkHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "cas ok keys: %A"
    // node.OnKVReadOkHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "read ok keys: %A"
    // node.OnKVErrorKeyDoesNotExistHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "error key does not exist keys: %A"
    // node.OnKVErrorPreconditionFailedHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "error precondition failed keys: %A"

    let node, messages = transition node action

    // node.OnKVWriteOkHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "write ok keys: %A"
    // node.OnKVCompareAndSwapOkHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "cas ok keys: %A"
    // node.OnKVReadOkHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "read ok keys: %A"
    // node.OnKVErrorKeyDoesNotExistHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "error key does not exist keys: %A"
    // node.OnKVErrorPreconditionFailedHandlers.Keys |> Seq.map string |> String.concat ","
    // |> eprintfn "error precondition failed keys: %A"

    node, messages