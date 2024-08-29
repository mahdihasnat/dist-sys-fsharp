[<AutoOpen>]
module GrowOnlyCounter.Types

//
// open System
// open FSharpPlus
// open FSharpPlus.Data
// open Types
// open Fleece
//
// type Delta = Delta of int
// with
//     static member get_Codec () : Codec<'a, Delta> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
//         Codec.isomorph (fun (Delta x) -> x) Delta Codecs.int
//
// type Value = Value of int
// with
//     static member get_Codec () : Codec<'a, Value> when 'a :> IEncoding and 'a : ( new : unit -> 'a) =
//         Codec.isomorph (fun (Value x) -> x) Value Codecs.int
//
// type SeqKVReadRPCError = SeqKVReadRPCError of unit
//
// [<RequireQualifiedAccess>]
// type InputMessageBody =
//     | Add of MessageId * Delta: Delta
//     | Read of MessageId
//     | OnSeqKVReadAck of MessageId * InReplyTo: MessageId * Result<Value, SeqKVReadRPCError>
//     | GossipAck of InReplyTo: MessageId
// with
//     static member get_Codec () =
//         codec {
//             let! (msgType) = jreqAlways "type" (function | Add _ -> "add" | Read _ -> "read" | OnSeqKVReadAck _ -> "gossip" | GossipAck _ -> "gossip_ok")
//             and! (messageId : Option<MessageId>) = jopt "msg_id" (function | Add (messageId, _) | Read messageId |  OnSeqKVReadAck (messageId, _) -> Some messageId | GossipAck _ -> None)
//             and! delta = jopt  "delta" (function | Add (_, delta) -> Some delta | Read _ | OnSeqKVReadAck _ | GossipAck _ -> None)
//             and! transactions = jopt "transactions" (function | OnSeqKVReadAck (_, messages) -> Some messages | Add _ | Read _ | GossipAck _-> None)
//             and! inReplyTo = jopt "in_reply_to" (function | GossipAck (inReplyTo) -> Some inReplyTo | _ -> None)
//             match msgType with
//             | s when s = "add" ->
//                 return Add(messageId |> Option.get, delta |> Option.get)
//             | s when s = "read" ->
//                 return Read (messageId |> Option.get)
//             | s when s = "gossip" ->
//                 return OnSeqKVReadAck (messageId |> Option.get, transactions |> Option.get)
//             | s when s = "gossip_ok" ->
//                 return GossipAck (inReplyTo |> Option.get)
//             | _ ->
//                 eprintfn $"Unknown message type: {msgType}"
//                 return failwithf $"Unknown message type: {msgType}"
//         }
//         |> ofObjCodec
//
// type OutputMessageBody =
//     | AddAck of InReplyTo: MessageId
//     | ReadAck of InReplyTo: MessageId * Value: Value
//     | GossipAck of InReplyTo: MessageId
//     | Gossip of MessageId: MessageId * transactions: NonEmptySet<Addition>
// with
//     static member get_Codec () =
//         codec {
//             let! msgType = jreqAlways "type" (function | ReadAck _ -> "read_ok" | AddAck _ -> "add_ok" | GossipAck _ -> "gossip_ok" | Gossip _ -> "gossip")
//             and! inReplyTo = jopt "in_reply_to" (function | ReadAck (inReplyTo, _) | AddAck inReplyTo | GossipAck inReplyTo -> Some inReplyTo | Gossip _ -> None)
//             and! value = jopt "value" (function | ReadAck (_, messages) -> Some messages | AddAck _ | GossipAck _  | Gossip _ -> None)
//             and! transactions = jopt "transactions" (function | Gossip (_, transactions) -> Some transactions | _ -> None)
//             and! messageId = jopt "msg_id" (function | Gossip (messageId, _) -> Some messageId | _ -> None)
//             match msgType with
//             | s when s = "read_ok" ->
//                 return ReadAck(inReplyTo |> Option.get, value |> Option.get)
//             | s when s = "add_ok" ->
//                 return AddAck (inReplyTo |> Option.get)
//             | s when s = "gossip_ok" ->
//                 return GossipAck (inReplyTo |> Option.get)
//             | s when s = "gossip" ->
//                 return Gossip (messageId |> Option.get, transactions |> Option.get)
//             | _ ->
//                 return failwithf $"invalid msgType {msgType}"
//         }
//         |> ofObjCodec
//
//
// type Node = {
//     Info : InitialNodeInfo
//     Neighbors: Set<NodeId>
//     Transactions: Set<Addition>
//     MessageCounter: int
//     PendingAck: Map<MessageId, (* DestinationNode *) NodeId * (* Transactions *) NonEmptySet<Addition> * (* MessageSentOn *) DateTimeOffset>
//     TimedOutMessages: Map<MessageId, (* DestinationNode *) NodeId * (* Transactions *) NonEmptySet<Addition>>
//     NeighborAckedMessages : Map<NodeId, Set<Addition>>
// }
