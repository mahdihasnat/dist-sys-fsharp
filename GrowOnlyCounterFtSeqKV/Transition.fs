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
        SeqKVOperation (KVOutputMessageBody.Read (queryMessageId, "sum"))
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
        withSeqKvRead node (fun node value ->
            let node =
                {
                    node with
                        ValueCache = value
                }
            (node, [])
        )
    | Choice1Of2 msg ->
        match msg.MessageBody with
        | Add(messageId, delta) ->
            let onSeqKVCompareAndSwapOk =
                fun (node: Node) ->
                    let node =
                            {
                                node with
                                    ValueCache = node.ValueCache + delta
                            }
                    let outputMessageBody: OutputMessageBody =
                        AddAck(messageId)
                    let outputMessage =
                        {
                            Source = node.Info.NodeId
                            Destination = msg.Source
                            MessageBody = outputMessageBody
                        }
                    node, [outputMessage]

            let rec addHandler node : Node * List<Message<OutputMessageBody>> =
                let newValue = node.ValueCache + delta
                let (node, updateMessageId: MessageId) = genMessageId node
                let updateMessageBody: OutputMessageBody =
                    SeqKVOperation (KVOutputMessageBody.CompareAndSwap (updateMessageId, "sum", node.ValueCache, newValue, node.ValueCache = Value 0))
                let updateMessage =
                    {
                        Source = node.Info.NodeId
                        Destination = NodeId.SeqKv
                        MessageBody = updateMessageBody
                    }

                let onSeqKVCompareAndSwapPreconditionFailed =
                    fun (node: Node) (value: Value) ->
                        let node =
                            {
                                node with
                                    ValueCache = value
                            }
                        addHandler node

                let node =
                    {
                        node with
                            OnSeqKVCompareAndSwapOkHandlers = node.OnSeqKVCompareAndSwapOkHandlers.Add(updateMessageId, onSeqKVCompareAndSwapOk)
                            OnSeqKVCompareAndSwapPreconditionFailedHandlers = node.OnSeqKVCompareAndSwapPreconditionFailedHandlers.Add(updateMessageId, onSeqKVCompareAndSwapPreconditionFailed)
                    }
                node, [updateMessage]

            addHandler node

        | Read messageId ->
            let outputMessageBody: OutputMessageBody =
                ReadAck(messageId, node.ValueCache)
            let outputMessage =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = outputMessageBody
                }
            node, [outputMessage]
        | OnSeqKVReadOk(inReplyTo, value) ->
            node.OnSeqKVReadOkHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            |> (fun f ->
                f node value
            )
        | OnSeqKVReadKeyDoesNotExist inReplyTo ->
            node.OnSeqKVReadKeyDoesNotExistHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            |> (fun f ->
                f node
            )

        | OnSeqKVCompareAndSwapOk inReplyTo ->
            node.OnSeqKVCompareAndSwapOkHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            |> (fun f ->
                f node
            )

        | OnSeqKVCompareAndSwapPreconditionFailed (inReplyTo, updatedValue) ->
            node.OnSeqKVCompareAndSwapPreconditionFailedHandlers
            |> Map.tryFind inReplyTo
            |> Option.get
            |> (fun f ->
                f node updatedValue
            )

