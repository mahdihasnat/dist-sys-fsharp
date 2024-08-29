[<AutoOpen>]
module GrowOnlyCounter.Program

open System
open System.Threading
open FSharpPlus
open FSharpPlus.Control
open Fleece.SystemTextJson
open Microsoft.FSharp.Control
open Types
open Utilities
open GrowOnlyCounter
open System.Threading.Tasks

open Utilities

open FsharpPlus
open Fleece.SystemTextJson
open Fleece

type Variant<^Variant1, ^Variant2, 'Encoding when
    'Encoding :> IEncoding and 'Encoding : (new : unit -> 'Encoding)
    and (Internals.GetCodec or ^Variant1) : (static member GetCodec: ^Variant1 * Internals.GetCodec * Internals.GetCodec * Internals.OpCodec -> Codec<'Encoding,^Variant1>)
    and (Internals.GetCodec or ^Variant2) : (static member GetCodec: ^Variant2 * Internals.GetCodec * Internals.GetCodec * Internals.OpCodec -> Codec<'Encoding,^Variant2>) > =
    | Variant1Of2 of ^Variant1
    | Variant2Of2 of ^Variant2
with
    static member inline get_Codec() =
        let codec1 : Codec<_, ^Variant1> = defaultCodec<_, ^Variant1>
        let codec2 : Codec<_, ^Variant2> = defaultCodec<_, ^Variant2>
        let encoder : Variant<^Variant1, ^Variant2, _> -> 'a =
            function
                | Variant1Of2 variant1 -> variant1 |> (Codec.encode codec1)
                | Variant2Of2 variant2 -> variant2 |> (Codec.encode codec2)
        let decoder1 : 'a -> ParseResult<Variant<^Variant1, ^Variant2, _>> =
            (Codec.decode codec1) >> (Result.map Variant1Of2)
        let decoder2 : 'a -> ParseResult<Variant<^Variant1, ^Variant2, _>> =
            (Codec.decode codec2) >> (Result.map Variant2Of2)

        Codec.create (encoder) (decoder1 <|> decoder2)
        // QQQ


type Foo =
    | Int of int
    | String of string
// with
//     static member get_Codec () =
//         let encoder =
//             function
//             | Int x -> JsonValue.Encode.int x
//             | String x -> JsonValue.Encode.string x
//
//         QQQ

[<EntryPoint>]
let main args =

    // printfn $"{toJsonText< Foo > (Int 1)}"
    printfn $"{toJsonText< Choice<int,string> > (Choice1Of2 1)}"
    // toJsonText< Foo > (String "hello")
    toJsonText< Choice<int,string> > (Choice2Of2 "hello")
    |> printfn "%s"

// open FSharpPlus
// open FSharpPlus.Data
// open Microsoft.FSharp.Core
// open Types
// open GrowOnlyCounter
//
// module Constants =
//     let maxOpenConnections = 2
//
// let removeTimeoutPendingAck (node: Node) : Node =
//     let now = DateTimeOffset.Now
//     let pendingMessages, timedOutMessages =
//         node.PendingAck
//         |> Map.partition (fun _ (_, _, sentOn) -> now < sentOn + (TimeSpan.FromMilliseconds 240))
//     let timedOutMessages =
//         timedOutMessages
//         |> Map.map (fun _ (destNode, transactions, _sentOn) -> (destNode, transactions))
//     {
//         node with
//             PendingAck = pendingMessages
//             TimedOutMessages = node.TimedOutMessages |> Map.toList |> List.append (timedOutMessages |> Map.toList) |> Map.ofList
//     }
//
// let transition (node: Node) (action: Choice<Message<InputMessageBody>,unit>) : Node * List<Message<OutputMessageBody>> =
//     let totalMessages= node.PendingAck.Count
//     let node = removeTimeoutPendingAck node
//     let currentMessages = node.PendingAck.Count
//     if totalMessages <> currentMessages then
//         eprintfn $"Message difference: {totalMessages - currentMessages}"
//     match action with
//     | Choice2Of2 unit ->
//         let now = DateTimeOffset.Now
//         let pendingConnectionCount: Map<NodeId, int> =
//             node.PendingAck
//             |> Map.toSeq
//             |> Seq.map (fun (_messageId, (nodeId, _, _)) -> nodeId)
//             |> Seq.groupBy id
//             |> Seq.map (fun (nodeId, nodeIds) -> (nodeId, nodeIds |> Seq.length))
//             |> Map.ofSeq
//         let pendingMessageCount: Map<NodeId, int> =
//             node.PendingAck
//             |> Map.toSeq
//             |> Seq.map (fun (_messageId, (nodeId, transactions, _)) -> (nodeId, transactions |> NonEmptySet.count))
//             |> Seq.groupBy fst
//             |> Seq.map (fun (nodeId, counts) -> (nodeId, counts |> Seq.map snd |> Seq.sum))
//             |> Map.ofSeq
//         let transactions, node =
//             node.Neighbors
//             |> Seq.filter (fun nodeId ->
//                 pendingConnectionCount.TryFind nodeId
//                 |> Option.defaultValue 0
//                 |> (>) Constants.maxOpenConnections
//             )
//             |> Seq.choose (fun neighNodeId ->
//                 let ackedMessages = (node.NeighborAckedMessages.TryFind neighNodeId |> Option.defaultValue Set.empty)
//                 let nonAckedRecentMessages : Set<Addition> =
//                     node.PendingAck
//                     |> Map.values
//                     |> Seq.choose (fun (nodeId, transactions, _) ->
//                         if nodeId = neighNodeId then
//                             Some (transactions |> NonEmptySet.toSet)
//                         else
//                             None
//                     )
//                     |> Seq.fold Set.union Set.empty
//
//                 (node.Transactions - ackedMessages) - nonAckedRecentMessages
//                 |> NonEmptySet.tryOfSet
//                 |> Option.filter (fun newMessages -> newMessages.Count > (pendingMessageCount.TryFind neighNodeId |> Option.defaultValue 0) * 3 / 4)
//                 |> Option.map (fun newMessages ->
//                     (neighNodeId, newMessages)
//                 )
//             )
//             |> Seq.mapFold (fun (node: Node) (nodeId, transactions) ->
//                 let messageId = MessageId node.MessageCounter
//                 let messageBody = OutputMessageBody.Gossip (messageId, transactions)
//                 let replyMessage: Message<OutputMessageBody> =
//                     {
//                         Source = node.Info.NodeId
//                         Destination = nodeId
//                         MessageBody = messageBody
//                     }
//                 (
//                  replyMessage,
//                     {
//                         node with
//                             MessageCounter = node.MessageCounter + 1
//                             PendingAck = node.PendingAck.Add (messageId, (nodeId, transactions, now))
//                     }
//                 )
//             ) node
//         (node, Seq.toList transactions)
//     | Choice1Of2 msg ->
//         match msg.MessageBody with
//         | InputMessageBody.Read messageId ->
//             let value: Value =
//                 node.Transactions
//                 |> Seq.map (fun { Delta = Delta x } -> x)
//                 |> Seq.sum
//                 |> Value
//             let replyMessageBody: OutputMessageBody =
//                 ReadAck (messageId, value)
//             let replyMessage: Message<OutputMessageBody> =
//                 {
//                     Source = node.Info.NodeId
//                     Destination = msg.Source
//                     MessageBody = replyMessageBody
//                 }
//             (node, [ replyMessage ])
//         | InputMessageBody.Add(messageId, delta) ->
//             let transaction = { Delta = delta; Guid = Guid.NewGuid() }
//             let replyMessageBody: OutputMessageBody =
//                 AddAck messageId
//             let replyMessage: Message<OutputMessageBody> =
//                 {
//                     Source = node.Info.NodeId
//                     Destination = msg.Source
//                     MessageBody = replyMessageBody
//                 }
//             let node =
//                 {
//                     node with
//                         Transactions = node.Transactions.Add transaction
//                 }
//             (node, [ replyMessage ])
//         | InputMessageBody.OnSeqKVReadAck(messageId, transactions) ->
//             let replyMessageBody: OutputMessageBody =
//                 GossipAck messageId
//             let replyMessage: Message<OutputMessageBody> =
//                 {
//                     Source = node.Info.NodeId
//                     Destination = msg.Source
//                     MessageBody = replyMessageBody
//                 }
//             let alreadyAckedMessages = node.NeighborAckedMessages.TryFind msg.Source |> Option.defaultValue Set.empty
//             let updatedAckedMessages =
//                 alreadyAckedMessages
//                 |> Set.union (transactions |> NonEmptySet.toSet)
//             let node =
//                 {
//                     node with
//                         Transactions = Set.union node.Transactions (transactions |> NonEmptySet.toSet)
//                         NeighborAckedMessages =
//                             node.NeighborAckedMessages.Add (msg.Source, updatedAckedMessages)
//                 }
//             (node, [ replyMessage ])
//         | InputMessageBody.GossipAck messageId ->
//             let node =
//                 node.PendingAck.TryFind messageId
//                 |> Option.map (fun (destNodeId, transactions, _) -> (destNodeId, transactions))
//                 |> Option.orElse (node.TimedOutMessages.TryFind messageId)
//                 |> Option.map (fun (nodeId, transactions) ->
//                     let updatedAckedMessages =
//                         node.NeighborAckedMessages.TryFind nodeId
//                         |> Option.defaultValue Set.empty
//                         |> Set.union (transactions |> NonEmptySet.toSet)
//                     {
//                         node with
//                             PendingAck = node.PendingAck.Remove messageId
//                             TimedOutMessages = node.TimedOutMessages.Remove messageId
//                             NeighborAckedMessages = node.NeighborAckedMessages.Add (nodeId, updatedAckedMessages)
//                     }
//                 )
//                 |> Option.defaultValue node
//             (node, [])


    // let semaphore: SemaphoreSlim = new SemaphoreSlim(1)
    // let nodeInfo = initNode ()
    // let nodeRef : ref<Node> =
    //     ref
    //         {
    //             Info = nodeInfo
    //             Transactions = Set.empty
    //             Neighbors =
    //                 generateGraph nodeInfo.ClusterNodeIds
    //                 |> Map.tryFind nodeInfo.NodeId
    //                 |> Option.defaultValue Set.empty
    //             MessageCounter = 0
    //             PendingAck = Map.empty
    //             TimedOutMessages = Map.empty
    //             NeighborAckedMessages = Map.empty
    //         }
    // let task1 = processStdin
    //                 (nodeRef, semaphore)
    //                 transition
    // let async1 = task1 |> Async.AwaitTask
    // let task2 = repeatSchedule
    //                 (TimeSpan.FromMilliseconds 100)
    //                 (nodeRef, semaphore)
    //                 (fun _ -> ())
    //                 transition
    // let async2 = task2 |> Async.AwaitTask
    // [| async2; async1 |]
    // |> Async.Parallel
    // |> Async.RunSynchronously
    // |> ignore

    0