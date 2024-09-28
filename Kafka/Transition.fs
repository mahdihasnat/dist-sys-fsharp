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
                    Offset messages.Length
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
                if node.CachedMessages.TryFind logKey |> Option.defaultValue List.empty |> List.contains (nextOffset, LogValue value) then
                    node.CachedMessages.TryFind logKey |> Option.defaultValue List.empty
                else
                    (nextOffset, LogValue value) :: (node.CachedMessages.TryFind logKey |> Option.defaultValue List.empty)

            let node = { node with CachedMessages = node.CachedMessages.Add(logKey, updatedLogs) }
            refreshLogsNext node
        )
        let node = node.RegisterErrorKeyDoesNotExistHandler queryMessageId f
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
        values
        |> List.map fst
        |> List.rev
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
            let writeLogOkHandler (node: Node) (offset: Offset) : TransitionResult =
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

            let incrementOffsetOkHandler (node: Node) (offset: Offset) : TransitionResult =
                let node, writeLogMessageId = genMessageId node
                let seqKVWriteMessageBody: OutputMessageBody =
                    KVRequest (KVRequestMessageBody.CompareAndSwap (writeLogMessageId, logIndex key offset, Value 0, Value value.Value, true))
                let seqKVWriteMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = NodeId.SeqKv
                        MessageBody = seqKVWriteMessageBody
                    }
                let node = node.RegisterCompareAndSwapOkHandler writeLogMessageId (fun node -> writeLogOkHandler node offset)
                eprintfn $"send: key: %A{key} value: %A{value} seq-kv write log in {offset} logIndex: {logIndex key offset}"
                node, [seqKVWriteMessage]

            let withLatestOffsetRead node (f: Node -> Offset -> TransitionResult) : TransitionResult =
                let node, queryMessageId = genMessageId node
                let linKVReadMessageBody: OutputMessageBody =
                    KVRequest (KVRequestMessageBody.Read (queryMessageId, key.Value))
                let linKVReadMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = NodeId.LinKv
                        MessageBody = linKVReadMessageBody
                    }

                let node = node.RegisterReadOkHandler queryMessageId (fun node (Value value) -> f node (Offset value))
                let node = node.RegisterErrorKeyDoesNotExistHandler queryMessageId (fun node -> f node (Offset -1))
                eprintfn $"send: key: %A{key} value: %A{value} lin-kv-read"
                node, [linKVReadMessage]

            let rec latestOffsetReadOkHandler (node: Node) (Offset offset) : TransitionResult =
                let nextOffset = Offset (offset + 1)
                let node, updateMessageId = genMessageId node
                let linKVWriteMessageBody: OutputMessageBody =
                    KVRequest (KVRequestMessageBody.CompareAndSwap (updateMessageId, key.Value, Value offset, Value nextOffset.Value, nextOffset.Value = 0))
                let linKVWriteMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = NodeId.LinKv
                        MessageBody = linKVWriteMessageBody
                    }
                let node = node.RegisterCompareAndSwapOkHandler updateMessageId (fun node -> incrementOffsetOkHandler node nextOffset)
                let node = node.RegisterErrorPreconditionFailedHandler updateMessageId (fun node -> withLatestOffsetRead node latestOffsetReadOkHandler)
                eprintfn $"send: key: %A{key} value: %A{value} cas write to {offset + 1}"
                node, [linKVWriteMessage]

            withLatestOffsetRead node latestOffsetReadOkHandler

        | InputMessageBody.Poll (messageId, offsets) ->

            let onRefreshLogsCompleted node : TransitionResult =
                let messages: Map<LogKey, List<Offset * LogValue>> =
                    offsets
                    |> Map.choosei (fun key offset ->
                        node.CachedMessages.TryFind key
                        |> Option.map (List.filter (fun (offset', _) -> offset <= offset') >> List.sortBy fst)
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
            let replyMessageBody: OutputMessageBody =
                CommitOffsetsAck (messageId)
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            let node =
                {
                    node with
                        CachedCommittedOffsets =
                            (node.CachedCommittedOffsets, offsets)
                            ||> Map.fold (fun acc key offset ->
                                match acc.TryFind key with
                                | Some offset' -> acc.Add (key, max offset offset')
                                | None -> acc.Add (key, offset)
                            )
                }
            (node, [ replyMessage ])
        | InputMessageBody.ListCommittedOffsets(messageId, keys) ->
            let offsets =
                keys
                |> NonEmptyList.toList
                |> List.choose (fun key ->
                    node.CachedCommittedOffsets.TryFind key
                    |> Option.map (fun offset -> (key, offset))
                )
                |> Map.ofList
            let replyMessageBody: OutputMessageBody =
                ListCommittedOffsetsAck (messageId, offsets)
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            (node, [ replyMessage ])
        | InputMessageBody.KVResponse response ->
            match response with
            | KVResponseMessageBody.ReadOk(inReplyTo, value) ->
                node.OnKVReadOkHandlers.TryFind inReplyTo
                |> Option.get
                |> fun f -> f node value
            | KVResponseMessageBody.ErrorKeyDoesNotExist inReplyTo ->
                node.OnKVErrorKeyDoesNotExistHandlers.TryFind inReplyTo
                |> Option.get
                <| node
            | KVResponseMessageBody.CompareAndSwapOk inReplyTo ->
                node.OnKVCompareAndSwapOkHandlers.TryFind inReplyTo
                |> Option.get
                <| node
            | KVResponseMessageBody.ErrorPreconditionFailed inReplyTo ->
                node.OnKVErrorPreconditionFailedHandlers.TryFind inReplyTo
                |> Option.get
                <| node

let transitionOuter (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : TransitionResult =
    let oldNode = node
    let node, messages = transition node action
    node, messages