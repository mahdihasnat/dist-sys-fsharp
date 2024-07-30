[<AutoOpen>]
module SingleNodeBroadcast.Types

open FSharpPlus
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
    | Topology of MessageId * Topology: Map<NodeId, List<NodeId>>
with
    static member get_Codec () =
        codec {
            let! (msgType) = jreqAlways "type" (function | BroadCast _ ->"broadcast" | Read _ -> "read" | Topology _ ->"topology")
            and! (messageId : MessageId) = jreqAlways "msg_id" (function | BroadCast (messageId, _) -> messageId | Read messageId -> messageId | Topology (messageId, _) ->messageId)
            and! (message : Option<int>) = jopt "message" (function BroadCast (_, message) -> Some message | _ -> None)
            and! topology = joptWith (Codecs.option (Codecs.propMapOfNodeId defaultCodec<_, List<NodeId>>))  "topology" (function Topology (_, topology) -> Some (topology)  | _ -> None)
            match msgType with
            | s when s = "broadcast" ->
                return BroadCast(messageId, message |> Option.get)
            | s when s = "read" ->
                return Read messageId
            | s when s = "topology" ->
                return Topology(messageId, topology |> Option.get)
            | _ ->
                return failwithf $"Unknown message type: {msgType}"
        }
        |> ofObjCodec

type Node = {
    Info : InitialNodeInfo
    Messages: Set<int>
}