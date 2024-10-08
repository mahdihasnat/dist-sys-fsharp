[<AutoOpen>]
module Kafka.Transition

open System
open FSharpPlus
open FSharpPlus.Data
open FSharpPlus.Lens
open Microsoft.FSharp.Core
open Types
open Kafka

let logIndex (key: LogKey) (offset: Offset) : string = $"{key.Value}-{offset.Value}"

let genMessageId (node: Node) : Node * MessageId =
    { node with
        NextMessageId = node.NextMessageId + 1
    },
    MessageId node.NextMessageId

let readFromKVService (node: Node) (target: NodeId) (key: string) (f: Node * Result<Value, unit> -> TransitionResult) : TransitionResult =
    transition {
        let node, queryMessageId = genMessageId node

        yield
            {
                Source = node.Info.NodeId
                Destination = target
                MessageBody = KVRequest (KVRequestMessageBody.Read (queryMessageId, key))
            }

        let node =
            node.RegisterReadOkHandler queryMessageId (fun node value -> f (node, Ok value))

        let node =
            node.RegisterErrorKeyDoesNotExistHandler queryMessageId (fun node -> f (node, Error ()))

        return node
    }

let readFromKVServiceInParallel
    (node: Node)
    (targets: NonEmptyList<NodeId * string>)
    (f: Node * NonEmptyList<Result<Value, unit>> -> TransitionResult)
    : TransitionResult =
    transition {
        let! node, revQueryMessageIds =
            targets
            |> NonEmptyList.toList
            |> List.fold
                (fun nodeAndMsgIdTR (destination, key) ->
                    transition {
                        let! node, queryMessageIds = nodeAndMsgIdTR
                        let node, queryMessageId = genMessageId node

                        let kvReadMessageBody: OutputMessageBody =
                            KVRequest (KVRequestMessageBody.Read (queryMessageId, key))

                        yield
                            {
                                Source = node.Info.NodeId
                                Destination = destination
                                MessageBody = kvReadMessageBody
                            }

                        return node, queryMessageId :: queryMessageIds
                    })
                ((node, []), [])

        let queryMessageResults: List<Choice<MessageId, Result<Value, unit>>> =
            revQueryMessageIds |> List.rev |> List.map Choice1Of2

        let rec handleResponse
            (results: List<Choice<MessageId, Result<Value, unit>>>)
            (node: Node)
            (queryId: MessageId, res: Result<Value, unit>)
            : TransitionResult =
            transition {
                let results =
                    results
                    |> List.map (fun state ->
                        match state with
                        | Choice1Of2 messageId -> if messageId = queryId then Choice2Of2 res else state
                        | Choice2Of2 valueResult -> state)

                let responses =
                    results
                    |> List.choose (fun state ->
                        match state with
                        | Choice2Of2 valueResult -> Some valueResult
                        | Choice1Of2 _ -> None)

                if responses.Length <> results.Length then
                    let node = registerAllCallbacks results node
                    return node
                else
                    return! f (node, responses |> NonEmptyList.ofList)
            }

        and registerAllCallbacks (results: List<Choice<MessageId, Result<Value, unit>>>) (node: Node) : Node =
            results
            |> List.fold
                (fun node state ->
                    match state with
                    | Choice1Of2 messageId ->
                        let node =
                            node.RegisterReadOkHandler messageId (fun node value -> handleResponse results node (messageId, Ok value))

                        let node =
                            node.RegisterErrorKeyDoesNotExistHandler messageId (fun node -> handleResponse results node (messageId, Error ()))

                        node
                    | Choice2Of2 valueResult -> node)
                node

        let node = registerAllCallbacks queryMessageResults node
        return node
    }

let writeToKVServiceAndForget (node: Node) (target: NodeId) (key: string) (value: Value) (f: Node -> TransitionResult) : TransitionResult =
    transition {
        let node, queryMessageId = genMessageId node

        yield
            {
                Source = node.Info.NodeId
                Destination = target
                MessageBody = KVRequest (KVRequestMessageBody.Write (queryMessageId, key, value))
            }

        let node = node.RegisterWriteOkHandler queryMessageId (fun node -> node, [])
        return! f node
    }

let getLogs (logKey: LogKey) (node: Node) (f: Node -> TransitionResult) : TransitionResult =
    let rec refreshLogsNext (offset: Offset) (node: Node) : TransitionResult =
        transition {
            let node, queryMessageId = genMessageId node
            let! node, res = readFromKVService node NodeId.SeqKv (logIndex logKey offset)

            match res with
            | Ok (Value value) ->
                let node =
                    { node with
                        CachedMessages =
                            node.CachedMessages.Add (
                                logKey,
                                node.CachedMessages.TryFind logKey
                                |> Option.defaultValue Map.empty
                                |> Map.add offset (LogValue value)
                            )
                    }

                return! refreshLogsNext (Offset (offset.Value + 1)) node
            | Error () ->
                eprintfn $"getLogs:: logKey: {logKey} offset: {offset} read failed"
                return! f node
        }

    refreshLogsNext
        (Offset
            (node.CachedMessages |> Map.tryFind logKey |> Option.defaultValue Map.empty)
                .Count)
        node

let rec refreshLogs (logKeys: List<LogKey>) node (f: Node -> TransitionResult) : TransitionResult =
    let (remainingKeys: ref<List<LogKey>>) = ref logKeys

    let rec refreshLogHandler (logKey: LogKey) (node: Node) : TransitionResult =
        remainingKeys.Value <- List.filter ((<>) logKey) remainingKeys.Value
        if remainingKeys.Value = [] then f node else node, []

    logKeys
    |> List.fold
        (fun nodeTR logKey ->
            transition {
                let! node = nodeTR
                let! node = getLogs logKey node (fun node -> refreshLogHandler logKey node)
                return node
            })
        (node, [])

let compareAndSwapToKVStore
    (target: NodeId)
    (key: string)
    (oldValue: Value)
    (newValue: Value)
    (createIfDoesNotExist: bool)
    (node: Node)
    (f: Node * Result<unit, unit> -> TransitionResult)
    : TransitionResult =
    transition {
        let node, updateMessageId = genMessageId node

        let linKVWriteMessageBody: OutputMessageBody =
            KVRequest (KVRequestMessageBody.CompareAndSwap (updateMessageId, key, oldValue, newValue, createIfDoesNotExist))

        yield
            {
                Source = node.Info.NodeId
                Destination = NodeId.LinKv
                MessageBody = linKVWriteMessageBody
            }

        let node =
            node.RegisterCompareAndSwapOkHandler updateMessageId (fun node -> f (node, Ok ()))

        let node =
            node.RegisterErrorPreconditionFailedHandler updateMessageId (fun node -> f (node, Error ()))

        return node
    }

let assertConsistency (node: Node) : unit =
    node.CachedMessages
    |> Map.forall (fun logKey values ->
        values.Keys
        |> Seq.toList
        |> fun offsets -> offsets = List.init offsets.Length (fun i -> Offset i))
    |> fun x ->
        eprintfn "assertConsistency: %A" x

        if x = false then
            failwithf "assertConsistency failed"

let transition (node: Node) (action: Choice<Message<InputMessageBody>, unit>) : TransitionResult =
    assertConsistency node

    transition {
        match action with
        | Choice2Of2 unit -> return node
        | Choice1Of2 msg ->
            match msg.MessageBody with
            | InputMessageBody.Send (messageId, key, value) ->
                // read current offset from lin-kv
                // increment current offsets from lin-kv
                // write log value to seq-kv

                let rec readAndIncrementOffset (node: Node) (f: Node * Offset -> TransitionResult) : TransitionResult =
                    transition {
                        let! node, res = readFromKVService node NodeId.LinKv $"current_offset_{key.Value}"

                        let oldOffset =
                            match res with
                            | Ok (Value value) -> Offset value
                            | Error () -> Offset -1

                        let newOffset = Offset (oldOffset.Value + 1)

                        let! node, res =
                            compareAndSwapToKVStore
                                NodeId.LinKv
                                $"current_offset_{key.Value}"
                                (Value oldOffset.Value)
                                (Value newOffset.Value)
                                (newOffset.Value = 0)
                                node

                        match res with
                        | Ok () -> return! f (node, newOffset)
                        | Error () -> return! readAndIncrementOffset node f
                    }

                let! node, offset = readAndIncrementOffset node
                let! node = writeToKVServiceAndForget node NodeId.SeqKv (logIndex key offset) (Value value.Value)

                yield
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = OutputMessageBody.SendAck (messageId, offset)
                    }

                return node

            | InputMessageBody.Poll (messageId, offsets) ->
                let! node = refreshLogs (offsets.Keys |> List.ofSeq) node

                let messages: Map<LogKey, List<Offset * LogValue>> =
                    offsets
                    |> Map.choosei (fun key offset ->
                        node.CachedMessages.TryFind key
                        |> Option.map (Map.filter (fun offset' _ -> offset <= offset') >> Map.toList))

                yield
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = PollAck (messageId, messages)
                    }

                return node

            | InputMessageBody.CommitOffsets (messageId, offsets) ->

                let! node =
                    offsets
                    |> Map.toList
                    |> List.fold
                        (fun (nodeResult: TransitionResult) (key, Offset offset) ->
                            transition {
                                let! node = nodeResult

                                let! node = writeToKVServiceAndForget node NodeId.LinKv ("committed_offset_" + key.Value) (Value offset)

                                return node
                            })
                        (transition { return node })

                yield
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = CommitOffsetsAck (messageId)
                    }

                return node

            | InputMessageBody.ListCommittedOffsets (messageId, keys) ->
                let! node, responses =
                    readFromKVServiceInParallel
                        node
                        (keys
                         |> NonEmptyList.map (fun key -> NodeId.LinKv, "committed_offset_" + key.Value))

                let offsets =
                    responses
                    |> NonEmptyList.zip keys
                    |> NonEmptyList.toList
                    |> List.choose (fun (key, response) ->
                        match response with
                        | Ok (Value value) -> Some (key, Offset value)
                        | Error () -> None)
                    |> Map.ofList

                yield
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = ListCommittedOffsetsAck (messageId, offsets)
                    }

                return node

            | InputMessageBody.KVResponse response ->
                return!
                    match response with
                    | KVResponseMessageBody.ReadOk (inReplyTo, value) ->
                        node.OnKVReadOkHandlers.TryFind inReplyTo
                        |> Option.get
                        |> fun f -> f (node.UnregisterAllHandler inReplyTo) value
                    | KVResponseMessageBody.WriteOk inReplyTo ->
                        node.OnKVWriteOkHandlers.TryFind inReplyTo |> Option.get
                        <| node.UnregisterAllHandler inReplyTo
                    | KVResponseMessageBody.ErrorKeyDoesNotExist inReplyTo ->
                        node.OnKVErrorKeyDoesNotExistHandlers.TryFind inReplyTo |> Option.get
                        <| node.UnregisterAllHandler inReplyTo
                    | KVResponseMessageBody.CompareAndSwapOk inReplyTo ->
                        node.OnKVCompareAndSwapOkHandlers.TryFind inReplyTo |> Option.get
                        <| node.UnregisterAllHandler inReplyTo
                    | KVResponseMessageBody.ErrorPreconditionFailed inReplyTo ->
                        node.OnKVErrorPreconditionFailedHandlers.TryFind inReplyTo |> Option.get
                        <| node.UnregisterAllHandler inReplyTo
    }

let transitionOuter (node: Node) (action: Choice<Message<InputMessageBody>, unit>) : TransitionResult =

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
