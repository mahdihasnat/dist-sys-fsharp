[<AutoOpen>]
module GrowOnlyCounter.Transition

open System
open Microsoft.FSharp.Core
open Types
open GrowOnlyCounter

let genMessageId (node: Node) : Node * MessageId =
    {
        node with
            NextMessageId = node.NextMessageId + 1
    },
    MessageId node.NextMessageId

let withSeqKvSingleRead node (f : Node -> Value * Version -> Node * List<Message<OutputMessageBody>>) : Node * List<Message<OutputMessageBody>> =
    let node, queryMessageId = genMessageId node
    let seqKVReadMessageBody: OutputMessageBody =
        SeqKVOperation (KVRequestMessageBody.Read (queryMessageId, "sum"))
    let seqKVReadMessage =
        {
            Source = node.Info.NodeId
            Destination = NodeId.SeqKv
            MessageBody = seqKVReadMessageBody
        }

    let onSeqKvReadOk =
        fun (node: Node) (value: Value, version: Version) ->
            f node (value, version)
    let onSeqKVReadKeyDoesNotExist =
        fun (node: Node) ->
            f node (Value 0, Version 0)
    let node =
        {
            node with
                OnSeqKVReadOkHandlers = node.OnSeqKVReadOkHandlers.Add(queryMessageId, onSeqKvReadOk)
                OnSeqKVReadKeyDoesNotExistHandlers = node.OnSeqKVReadKeyDoesNotExistHandlers.Add(queryMessageId, onSeqKVReadKeyDoesNotExist)
        }
    node, [seqKVReadMessage]

let withTransformValueAndVersion node (mapper: Value * Version -> Value * Version) (f: Node -> Value * Version -> Node * List<Message<OutputMessageBody>>) : Node * List<Message<OutputMessageBody>> =
    let rec writeTransformedValue node (oldValueAndVersion) : Node * List<Message<OutputMessageBody>> =
        let (node, updateMessageId: MessageId) = genMessageId node
        let newValueAndVersion = mapper oldValueAndVersion
        let updateMessageBody: OutputMessageBody =
            SeqKVOperation (KVRequestMessageBody.CompareAndSwap (updateMessageId, "sum", oldValueAndVersion,  newValueAndVersion, fst oldValueAndVersion = Value 0))
        let updateMessage =
            {
                Source = node.Info.NodeId
                Destination = NodeId.SeqKv
                MessageBody = updateMessageBody
            }

        let node =
            {
                node with
                    OnSeqKVCompareAndSwapOkHandlers = node.OnSeqKVCompareAndSwapOkHandlers.Add(updateMessageId, (fun node -> f node newValueAndVersion))
                    OnSeqKVCompareAndSwapPreconditionFailedHandlers = node.OnSeqKVCompareAndSwapPreconditionFailedHandlers.Add(updateMessageId, fun node -> withSeqKvSingleRead node writeTransformedValue)
            }
        node, [updateMessage]

    withSeqKvSingleRead node writeTransformedValue

let inline transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : Node * List<Message<OutputMessageBody>> =
    match action with
    | Choice2Of2 unit ->
        node, []
    | Choice1Of2 msg ->
        match msg.MessageBody with
        | Add(messageId, delta) ->
            let replyAddOk node =
                let outputMessageBody: OutputMessageBody =
                    AddAck(messageId)
                let outputMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = outputMessageBody
                    }
                node, [outputMessage]

            if delta = Delta 0 then
                replyAddOk node
            else
                withTransformValueAndVersion node (fun (value, _version) -> (value + delta, Version 0)) (fun node _ -> replyAddOk node)
        | Read messageId ->
            let replyReadOk node (value, _version) =
                let outputMessageBody: OutputMessageBody =
                    ReadAck(messageId, value)
                let outputMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = outputMessageBody
                    }
                node, [outputMessage]
            withTransformValueAndVersion node (fun (value, version) -> (value, version + 1)) replyReadOk
        | KVResponse (KVResponseMessageBody.ReadOk(inReplyTo, value)) ->
            node.OnSeqKVReadOkHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            |> (fun f ->
                f node value
            )
        | KVResponse (KVResponseMessageBody.ErrorKeyDoesNotExist inReplyTo) ->
            node.OnSeqKVReadKeyDoesNotExistHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            <| node

        | KVResponse (KVResponseMessageBody.CompareAndSwapOk inReplyTo) ->
            node.OnSeqKVCompareAndSwapOkHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            <| node

        | KVResponse (KVResponseMessageBody.ErrorPreconditionFailed inReplyTo) ->
            node.OnSeqKVCompareAndSwapPreconditionFailedHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            <| node


