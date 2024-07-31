[<AutoOpen>]
module BroadCast.Types

open System
open System.Runtime.InteropServices.ObjectiveC
open FSharpPlus
open FSharpPlus.Data
open Types
open Fleece

type BroadCastMessageBody = {
    Message: int
    MessageId: MessageId
}
with
    static member get_ObjCodec () =
        codec {
            let! _type = jreq "type" (fun x -> Some "broadcast")
            and! messageId = jreq "msg_id" (fun x -> Some x.MessageId)
            and! message = jreq "message" (fun x -> Some x.Message)
            return {
                Message = message
                MessageId = messageId
            }
        }
    static member get_Codec () =
        BroadCastMessageBody.get_ObjCodec ()
        |> ofObjCodec

type BroadCastReplyMessageBody = {
    InReplyTo: MessageId
}
with
    static member get_Codec () =
        codec {
            let! _type = jreq "type" (fun _ -> Some "broadcast_ok")
            and! inReplyTo = jreq "in_reply_to" (fun x -> Some x.InReplyTo)
            return {
                InReplyTo = inReplyTo
            }
        }
        |> ofObjCodec

type ReadMessageBody = {
    MessageId : MessageId
}
with
    static member get_ObjCodec () =
        codec {
            let! _type = jreq "type" (fun _ -> Some "read")
            and! messageId = jreq "msg_id" (fun x -> Some x.MessageId)
            return {
                MessageId = messageId
            }
        }
    static member get_Codec () =
        ReadMessageBody.get_ObjCodec ()
        |> ofObjCodec

type ReadReplyMessageBody = {
    InReplyTo: MessageId
    Messages : Set<int>
}
with
    static member get_Codec () =
        codec {
            let! _type = jreq "type" (fun _ -> Some "read_ok")
            and! inReplyTo = jreq "in_reply_to" (fun x -> Some x.InReplyTo)
            and! messages = jreq "messages" (fun x -> Some x.Messages)
            return {
                InReplyTo = inReplyTo
                Messages = messages
            }
        }
        |> ofObjCodec

type TopologyMessageBody = {
    MessageId : MessageId
    Topology: Map<NodeId, List<NodeId>>
}
with
    static member get_ObjCodec () =
        codec {
            let! _type = jreq "type" (fun _ -> Some "topology")
            and! topology = jreq "topology" (fun x -> Some x.Topology)
            and! messageId = jreq "msg_id" (fun x -> Some x.MessageId)
            return {
                MessageId = messageId
                Topology = topology
            }
        }
    static member get_Codec () =
        TopologyMessageBody.get_ObjCodec ()
        |> ofObjCodec

type TopologyReplyMessageBody = {
    InReplyTo: MessageId
}
with
    static member get_Codec () =
        codec {
            let! _type = jreq "type" (fun _ -> Some "topology_ok")
            and! inReplyTo = jreq "in_reply_to" (fun x -> Some x.InReplyTo)
            return {
                InReplyTo = inReplyTo
            }
        }
        |> ofObjCodec

module Codecs =
    let propMapOfNodeId (codec: Codec<'Encoding, 'a>) = (Ok << Map.ofSeq << Seq.map (Tuple2.mapItem1 NodeId) <<PropertyList.ToSeq <-> (Map.toArray >> Array.map (Tuple2.mapItem1 (fun (NodeId x) -> x)) >> PropertyList)) >.> Codecs.propList codec

type CodecApplicativeBuilderExtension () =
    member _.Bind() = QQQ

[<RequireQualifiedAccess>]
type InputMessageBody =
    | BroadCast of MessageId * Message: int
    | Read of MessageId
    | Topology of MessageId * Topology: Map<NodeId, Set<NodeId>>
    | Gossip of MessageId * Messages: NonEmptySet<int>
    | GossipAck of InReplyTo: MessageId
with
    static member get_Codec () =
        codec {
            let! (msgType) = jreqAlways "type" (function | BroadCast _ ->"broadcast" | Read _ -> "read" | Topology _ ->"topology" | Gossip _ -> "gossip" | GossipAck _ -> "gossip_ok")
            and! (messageId : Option<MessageId>) = jopt "msg_id" (function | BroadCast (messageId, _) | Read messageId | Topology (messageId, _) |  Gossip (messageId, _) -> Some messageId | GossipAck _ -> None)
            and! (message : Option<int>) = jopt "message" (function BroadCast (_, message) -> Some message | Gossip _ | Read _ | Topology _ | GossipAck _ -> None)
            and! topology = joptWith (Codecs.option (Codecs.propMapOfNodeId defaultCodec<_, Set<NodeId>>))  "topology" (function Topology (_, topology) -> Some (topology)  | _ -> None)
            and! messages = jopt "messages" (function | Gossip (_, messages) -> Some messages | BroadCast _ | Read _ | Topology _ | GossipAck _-> None)
            and! inReplyTo = jopt "in_reply_to" (function | GossipAck (inReplyTo) -> Some inReplyTo | _ -> None)
            match msgType with
            | s when s = "broadcast" ->
                return BroadCast(messageId |> Option.get, message |> Option.get)
            | s when s = "read" ->
                return Read (messageId |> Option.get)
            | s when s = "topology" ->
                return Topology(messageId |> Option.get, topology |> Option.get)
            | s when s = "gossip" ->
                return Gossip (messageId |> Option.get, messages |> Option.get)
            | s when s = "gossip_ok" ->
                return GossipAck (inReplyTo |> Option.get)
            | _ ->
                return failwithf $"Unknown message type: {msgType}"
        }
        |> ofObjCodec

type OutputMessageBody =
    | ReadAck of InReplyTo: MessageId * Messages: Set<int>
    | BroadCastAck of InReplyTo: MessageId
    | TopologyAck of InReplyTo: MessageId
    | GossipAck of InReplyTo: MessageId
    | Gossip of MessageId: MessageId * Messages: NonEmptySet<int>
with
    static member get_Codec () =
        codec {
            let! msgType = jreqAlways "type" (function | ReadAck _ -> "read_ok" | BroadCastAck _ -> "broadcast_ok" | TopologyAck _ -> "topology_ok" | GossipAck _ -> "gossip_ok" | Gossip _ -> "gossip")
            and! inReplyTo = jopt "in_reply_to" (function | ReadAck (inReplyTo, _) | BroadCastAck inReplyTo | TopologyAck inReplyTo | GossipAck inReplyTo -> Some inReplyTo | Gossip _ -> None)
            and! messages = jopt "messages" (function | ReadAck (_, messages) -> Some messages | BroadCastAck _ | TopologyAck _ | GossipAck _  | Gossip _ -> None)
            and! gossipMessages = jopt "messages" (function | Gossip (_, messages) -> Some messages | _ -> None)
            and! messageId = jopt "msg_id" (function | Gossip (messageId, _) -> Some messageId | _ -> None)
            match msgType with
            | s when s = "read_ok" ->
                return ReadAck(inReplyTo |> Option.get, messages |> Option.get)
            | s when s = "broadcast_ok" ->
                return BroadCastAck (inReplyTo |> Option.get)
            | s when s = "topology_ok" ->
                return TopologyAck (inReplyTo |> Option.get)
            | s when s = "gossip_ok" ->
                return GossipAck (inReplyTo |> Option.get)
            | s when s = "gossip" ->
                return Gossip (messageId |> Option.get, gossipMessages |> Option.get)
            | _ ->
                return failwithf $"invalid msgType {msgType}"
        }
        |> ofObjCodec
type Node = {
    Info : InitialNodeInfo
    Messages: Set<int>
    Neighbors: Set<NodeId>
    MessageCounter: int
    PendingAck: Map<MessageId, (* DestinationNode *) NodeId * (* Messages *) NonEmptySet<int> * (* MessageSentOn *) DateTimeOffset>
    NeighborAckedMessages : Map<NodeId, Set<int>>
}
