[<AutoOpen>]
module BroadCast.Transition

open FSharpPlus.Data
open Types
open BroadCast

let transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : Node * List<Message<OutputMessageBody>> =
    match action with
    | Choice2Of2 unit ->
        let messages, node =
            node.Neighbors
            |> Set.toSeq
            |> Seq.choose (fun nodeId ->
                node.Messages - (node.NeighborAckedMessages.TryFind nodeId |> Option.defaultValue Set.empty)
                |> NonEmptySet.tryOfSet
                |> Option.map (fun newMessages ->
                    (nodeId, newMessages)
                )
            )
            |> Seq.mapFold (fun (node: Node) (nodeId, messages) ->
                let messageId = MessageId node.MessageCounter
                let messageBody = OutputMessageBody.Gossip (messageId, messages)
                let replyMessage: Message<OutputMessageBody> =
                    {
                        Source = node.Info.NodeId
                        Destination = nodeId
                        MessageBody = messageBody
                    }
                (
                 replyMessage,
                    {
                        node with
                            MessageCounter = node.MessageCounter + 1
                            PendingAck = node.PendingAck.Add (messageId, (nodeId, messages))
                    }
                )
            ) node
        (node, Seq.toList messages)
    | Choice1Of2 msg ->
        match msg.MessageBody with
        | InputMessageBody.Read messageId ->
            let replyMessageBody: OutputMessageBody =
                ReadAck (messageId, node.Messages)
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            (node, [ replyMessage ])
        | InputMessageBody.BroadCast(messageId, message) ->
            let replyMessageBody: OutputMessageBody =
                BroadCastAck messageId
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            let node =
                {
                    node with
                        Messages = node.Messages.Add message
                }
            (node, [ replyMessage ])
        | InputMessageBody.Topology(messageId, topology) ->
            let replyMessageBody: OutputMessageBody =
                TopologyAck messageId
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            let node =
                {
                    node with
                        Neighbors = topology.TryFind node.Info.NodeId |> Option.defaultValue Set.empty
                }
            (node, [ replyMessage ])
        | InputMessageBody.Gossip(messageId, messages) ->
            let replyMessageBody: OutputMessageBody =
                GossipAck messageId
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            let node =
                {
                    node with
                        Messages = Set.union node.Messages (messages |> NonEmptySet.toSet)
                }
            (node, [ replyMessage ])
        | InputMessageBody.GossipAck messageId ->
            let node =
                node.PendingAck.TryFind messageId
                |> Option.map (fun (nodeId, messages) ->
                    let updatedAckedMessages =
                        node.NeighborAckedMessages.TryFind nodeId
                        |> Option.defaultValue Set.empty
                        |> Set.union (messages |> NonEmptySet.toSet)
                    {
                        node with
                            PendingAck = node.PendingAck.Remove messageId
                            NeighborAckedMessages = node.NeighborAckedMessages.Add (nodeId, updatedAckedMessages)
                    }
                )
                |> Option.defaultValue node
            (node, [])
