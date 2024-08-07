[<AutoOpen>]
module BroadCast.Transition

open System
open FSharpPlus
open FSharpPlus.Data
open Microsoft.FSharp.Core
open Types
open BroadCast

let removeTimeoutPendingAck (node: Node) : Node =
    let now = DateTimeOffset.Now
    let pendingMessages, timedOutMessages =
        node.PendingAck
        |> Map.partition (fun _ (_, _, sentOn) -> now < sentOn + (TimeSpan.FromMilliseconds 210))
    let timedOutMessages =
        timedOutMessages
        |> Map.map (fun _ (destNode, messages, _sentOn) -> (destNode, messages))
    {
        node with
            PendingAck = pendingMessages
            TimedOutMessages = node.TimedOutMessages |> Map.toList |> List.append (timedOutMessages |> Map.toList) |> Map.ofList
    }

let generateGraph (nodes: NonEmptySet<NodeId>) : Map<NodeId, Set<NodeId>> =
    let nodes = nodes |> NonEmptySet.toSeq
    let n = nodes |> Seq.length
    let k = Math.Sqrt (float n) |> int
    let blocks = nodes |> Seq.chunkBySize k
    let edgesWithinBlock : seq<NodeId * NodeId> =
        blocks
        |> Seq.collect (fun nodes -> Seq.allPairs nodes nodes)
    let edgesBetweenBlocks : seq<NodeId * NodeId> =
        blocks
        |> Seq.collect (Seq.indexed)
        |> Seq.groupBy fst
        |> Seq.map snd
        |> Seq.map (Seq.map snd)
        |> Seq.collect (fun nodes -> Seq.allPairs nodes nodes)
    Seq.append edgesWithinBlock edgesBetweenBlocks
    |> Seq.groupBy fst
    |> Seq.map (fun (src, dests) -> src, dests |> Seq.map snd |> Set.ofSeq |> Set.remove src)
    |> Map.ofSeq

let transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : Node * List<Message<OutputMessageBody>> =
    let totalMessages= node.PendingAck.Count
    let node = removeTimeoutPendingAck node
    let currentMessages = node.PendingAck.Count
    if totalMessages <> currentMessages then
        eprintfn $"Message difference: {totalMessages - currentMessages}"
    match action with
    | Choice2Of2 unit ->
        let now = DateTimeOffset.Now
        let pendingAckNodes =
            node.PendingAck.Values
            |> Seq.map (fun (nodeId, _, _) -> nodeId)
            |> Set.ofSeq
        let messages, node =
            Set.difference node.Neighbors pendingAckNodes
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
                        Neighbors =
                            // node.Info.ClusterNodeIds |> NonEmptySet.toSet |> Set.remove node.Info.NodeId // too much megs-per-op
                            // topology.TryFind node.Info.NodeId |> Option.defaultValue Set.empty // too much stable latency
                            generateGraph node.Info.ClusterNodeIds
                            |> Map.tryFind node.Info.NodeId
                            |> Option.defaultValue Set.empty
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
            let alreadyAckedMessages = node.NeighborAckedMessages.TryFind msg.Source |> Option.defaultValue Set.empty
            let updatedAckedMessages =
                alreadyAckedMessages
                |> Set.union (messages |> NonEmptySet.toSet)
            let node =
                {
                    node with
                        Messages = Set.union node.Messages (messages |> NonEmptySet.toSet)
                        NeighborAckedMessages =
                            node.NeighborAckedMessages.Add (msg.Source, updatedAckedMessages)
                }
            (node, [ replyMessage ])
        | InputMessageBody.GossipAck messageId ->
            let node =
                node.PendingAck.TryFind messageId
                |> Option.map (fun (destNodeId, messages, _) -> (destNodeId, messages))
                |> Option.orElse (node.TimedOutMessages.TryFind messageId)
                |> Option.map (fun (nodeId, messages) ->
                    let updatedAckedMessages =
                        node.NeighborAckedMessages.TryFind nodeId
                        |> Option.defaultValue Set.empty
                        |> Set.union (messages |> NonEmptySet.toSet)
                    {
                        node with
                            PendingAck = node.PendingAck.Remove messageId
                            TimedOutMessages = node.TimedOutMessages.Remove messageId
                            NeighborAckedMessages = node.NeighborAckedMessages.Add (nodeId, updatedAckedMessages)
                    }
                )
                |> Option.defaultValue node
            (node, [])
