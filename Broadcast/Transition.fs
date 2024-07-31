[<AutoOpen>]
module BroadCast.Transition

open System
open FSharpPlus.Data
open Types
open BroadCast

let removeTimeoutPendingAck (node: Node) : Node =
    let now = DateTimeOffset.Now
    {
        node with
            PendingAck = node.PendingAck |> Map.filter (fun _ (_, _, sentOn) -> now < sentOn + (TimeSpan.FromMilliseconds 100))
    }

let transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : Node * List<Message<OutputMessageBody>> =
    let node = removeTimeoutPendingAck node
    match action with
    | Choice2Of2 unit ->
        let now = DateTimeOffset.Now
        let messages, node =
            node.Neighbors
            |> Set.toSeq
            |> Seq.choose (fun neighNodeId ->
                let ackedMessages = (node.NeighborAckedMessages.TryFind neighNodeId |> Option.defaultValue Set.empty)
                let nonAckedRecentMessages : Set<int> =
                    node.PendingAck
                    |> Map.values
                    |> Seq.choose (fun (nodeId, messages, _) ->
                        if nodeId = neighNodeId then
                            Some (messages |> NonEmptySet.toSet)
                        else
                            None
                    )
                    |> Seq.fold Set.union Set.empty
                (node.Messages - ackedMessages) - nonAckedRecentMessages
                |> NonEmptySet.tryOfSet
                |> Option.map (fun newMessages ->
                    (neighNodeId, newMessages)
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
                            PendingAck = node.PendingAck.Add (messageId, (nodeId, messages, now))
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
                |> Option.map (fun (nodeId, messages, _) ->
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
