[<AutoOpen>]
module GrowOnlyCounter.Transition

open System
open FSharpPlus
open FSharpPlus.Data
open Microsoft.FSharp.Core
open Types
open GrowOnlyCounter

let genMessageId (node: Node) : Node * MessageId =
    {
        node with
            NextMessageId = node.NextMessageId + 1
    },
    MessageId node.NextMessageId

let withSeqKvRead node (f : Node -> Value -> Node * List<Message<OutputMessageBody>>) : Node * List<Message<OutputMessageBody>> =
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
        fun (node: Node) (value: Value) ->
            f node value
    let onSeqKVReadKeyDoesNotExist =
        fun (node: Node) ->
            f node (Value 0)
    let node =
        {
            node with
                OnSeqKVReadOkHandlers = node.OnSeqKVReadOkHandlers.Add(queryMessageId, onSeqKvReadOk)
                OnSeqKVReadKeyDoesNotExistHandlers = node.OnSeqKVReadKeyDoesNotExistHandlers.Add(queryMessageId, onSeqKVReadKeyDoesNotExist)
        }
    node, [seqKVReadMessage]

let inline transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : Node * List<Message<OutputMessageBody>> =
    match action with
    | Choice2Of2 unit ->
        node, []
    | Choice1Of2 msg ->
        match msg.MessageBody with
        | Add(messageId, delta) ->
            let onSeqKVCompareAndSwapOk node =
                let outputMessageBody: OutputMessageBody =
                    AddAck(messageId)
                let outputMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = outputMessageBody
                    }
                node, [outputMessage]

            let rec addHandler node value : Node * List<Message<OutputMessageBody>> =
                let newValue = delta + value
                let (node, updateMessageId: MessageId) = genMessageId node
                let updateMessageBody: OutputMessageBody =
                    SeqKVOperation (KVRequestMessageBody.CompareAndSwap (updateMessageId, "sum", value, newValue, value = Value 0))
                let updateMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = NodeId.SeqKv
                        MessageBody = updateMessageBody
                    }

                let node =
                    {
                        node with
                            OnSeqKVCompareAndSwapOkHandlers = node.OnSeqKVCompareAndSwapOkHandlers.Add(updateMessageId, onSeqKVCompareAndSwapOk)
                            OnSeqKVCompareAndSwapPreconditionFailedHandlers = node.OnSeqKVCompareAndSwapPreconditionFailedHandlers.Add(updateMessageId, addHandler)
                    }
                node, [updateMessage]
            withSeqKvRead node addHandler
        | Read messageId ->
            let replyReadOk node value =
                let outputMessageBody: OutputMessageBody =
                    ReadAck(messageId, value)
                let outputMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = outputMessageBody
                    }
                node, [outputMessage]
            withSeqKvRead node replyReadOk
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
            |> (fun f ->
                f node
            )

        | KVResponse (KVResponseMessageBody.CompareAndSwapOk inReplyTo) ->
            node.OnSeqKVCompareAndSwapOkHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            |> (fun f ->
                f node
            )

        | KVResponse (KVResponseMessageBody.ErrorPreconditionFailed(inReplyTo, updatedValue)) ->
            node.OnSeqKVCompareAndSwapPreconditionFailedHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            |> (fun f ->
                f node updatedValue
            )

