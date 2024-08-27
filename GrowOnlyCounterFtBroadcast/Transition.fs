[<AutoOpen>]
module GrowOnlyCounter.Transition

open System
open FSharpPlus
open FSharpPlus.Data
open Microsoft.FSharp.Core
open Types
open GrowOnlyCounter

module Constants =
    let maxOpenConnections = 2

let removeTimeoutPendingAck (node: Node) : Node =
    let now = DateTimeOffset.Now
    let pendingMessages, timedOutMessages =
        node.PendingAck
        |> Map.partition (fun _ (_, _, sentOn) -> now < sentOn + (TimeSpan.FromMilliseconds 240))
    let timedOutMessages =
        timedOutMessages
        |> Map.map (fun _ (destNode, transactions, _sentOn) -> (destNode, transactions))
    {
        node with
            PendingAck = pendingMessages
            TimedOutMessages = node.TimedOutMessages |> Map.toList |> List.append (timedOutMessages |> Map.toList) |> Map.ofList
    }

let transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : Node * List<Message<OutputMessageBody>> =
    let totalMessages= node.PendingAck.Count
    let node = removeTimeoutPendingAck node
    let currentMessages = node.PendingAck.Count
    if totalMessages <> currentMessages then
        eprintfn $"Message difference: {totalMessages - currentMessages}"
    match action with
    | Choice2Of2 unit ->
        let now = DateTimeOffset.Now
        let pendingConnectionCount: Map<NodeId, int> =
            node.PendingAck
            |> Map.toSeq
            |> Seq.map (fun (_messageId, (nodeId, _, _)) -> nodeId)
            |> Seq.groupBy id
            |> Seq.map (fun (nodeId, nodeIds) -> (nodeId, nodeIds |> Seq.length))
            |> Map.ofSeq
        let pendingMessageCount: Map<NodeId, int> =
            node.PendingAck
            |> Map.toSeq
            |> Seq.map (fun (_messageId, (nodeId, transactions, _)) -> (nodeId, transactions |> NonEmptySet.count))
            |> Seq.groupBy fst
            |> Seq.map (fun (nodeId, counts) -> (nodeId, counts |> Seq.map snd |> Seq.sum))
            |> Map.ofSeq
        let transactions, node =
            node.Neighbors
            |> Seq.filter (fun nodeId ->
                pendingConnectionCount.TryFind nodeId
                |> Option.defaultValue 0
                |> (>) Constants.maxOpenConnections
            )
            |> Seq.choose (fun neighNodeId ->
                let ackedMessages = (node.NeighborAckedMessages.TryFind neighNodeId |> Option.defaultValue Set.empty)
                let nonAckedRecentMessages : Set<Addition> =
                    node.PendingAck
                    |> Map.values
                    |> Seq.choose (fun (nodeId, transactions, _) ->
                        if nodeId = neighNodeId then
                            Some (transactions |> NonEmptySet.toSet)
                        else
                            None
                    )
                    |> Seq.fold Set.union Set.empty

                (node.Transactions - ackedMessages) - nonAckedRecentMessages
                |> NonEmptySet.tryOfSet
                |> Option.filter (fun newMessages -> newMessages.Count > (pendingMessageCount.TryFind neighNodeId |> Option.defaultValue 0) * 3 / 4)
                |> Option.map (fun newMessages ->
                    (neighNodeId, newMessages)
                )
            )
            |> Seq.mapFold (fun (node: Node) (nodeId, transactions) ->
                let messageId = MessageId node.MessageCounter
                let messageBody = OutputMessageBody.Gossip (messageId, transactions)
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
                            PendingAck = node.PendingAck.Add (messageId, (nodeId, transactions, now))
                    }
                )
            ) node
        (node, Seq.toList transactions)
    | Choice1Of2 msg ->
        match msg.MessageBody with
        | InputMessageBody.Read messageId ->
            let value: Value =
                node.Transactions
                |> Seq.map (fun { Delta = Delta x } -> x)
                |> Seq.sum
                |> Value
            let replyMessageBody: OutputMessageBody =
                ReadAck (messageId, value)
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            (node, [ replyMessage ])
        | InputMessageBody.Add(messageId, delta) ->
            let transaction = { Delta = delta; Guid = Guid.NewGuid() }
            let replyMessageBody: OutputMessageBody =
                AddAck messageId
            let replyMessage: Message<OutputMessageBody> =
                {
                    Source = node.Info.NodeId
                    Destination = msg.Source
                    MessageBody = replyMessageBody
                }
            let node =
                {
                    node with
                        Transactions = node.Transactions.Add transaction
                }
            (node, [ replyMessage ])
        | InputMessageBody.Gossip(messageId, transactions) ->
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
                |> Set.union (transactions |> NonEmptySet.toSet)
            let node =
                {
                    node with
                        Transactions = Set.union node.Transactions (transactions |> NonEmptySet.toSet)
                        NeighborAckedMessages =
                            node.NeighborAckedMessages.Add (msg.Source, updatedAckedMessages)
                }
            (node, [ replyMessage ])
        | InputMessageBody.GossipAck messageId ->
            let node =
                node.PendingAck.TryFind messageId
                |> Option.map (fun (destNodeId, transactions, _) -> (destNodeId, transactions))
                |> Option.orElse (node.TimedOutMessages.TryFind messageId)
                |> Option.map (fun (nodeId, transactions) ->
                    let updatedAckedMessages =
                        node.NeighborAckedMessages.TryFind nodeId
                        |> Option.defaultValue Set.empty
                        |> Set.union (transactions |> NonEmptySet.toSet)
                    {
                        node with
                            PendingAck = node.PendingAck.Remove messageId
                            TimedOutMessages = node.TimedOutMessages.Remove messageId
                            NeighborAckedMessages = node.NeighborAckedMessages.Add (nodeId, updatedAckedMessages)
                    }
                )
                |> Option.defaultValue node
            (node, [])
