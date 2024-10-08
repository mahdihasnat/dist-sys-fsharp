[<AutoOpen>]
module Types.Message

open Fleece
open FSharpPlus.Data
open FSharpPlus

type MessageId =
    | MessageId of int

    static member get_Codec () : Codec<_, MessageId> =
        Codec.isomorph (fun (MessageId x) -> x) (fun x -> MessageId x) Codecs.int

type InitMessage =
    {
        MessageId: MessageId
        NodeId: NodeId
        AllNodeIds: NonEmptySet<NodeId>
    }

    static member get_Codec () : Codec<_, InitMessage> =
        codec {
            let! _type = jreq "type" (fun x -> Some "init")
            and! messageId = jreq "msg_id" (fun x -> Some x.MessageId)
            and! nodeId = jreq "node_id" (fun x -> Some x.NodeId)
            and! allNodeIds = jreq "node_ids" (fun x -> Some x.AllNodeIds)

            return
                {
                    MessageId = messageId
                    NodeId = nodeId
                    AllNodeIds = allNodeIds
                }
        }
        |> ofObjCodec

type InitReplyMessage =
    {
        InReplyTo: MessageId
    }

    static member get_Codec () : Codec<'Encoding, InitReplyMessage> when 'Encoding :> IEncoding and 'Encoding: (new: unit -> 'Encoding) =
        codec {
            let! _type = jreq "type" (fun x -> Some "init_ok")
            and! inReplyTo = jreq "in_reply_to" (fun x -> Some x.InReplyTo)
            return { InReplyTo = inReplyTo }
        }
        |> ofObjCodec

type Message<'MessageBodyType> =
    {
        Source: NodeId
        Destination: NodeId
        MessageBody: 'MessageBodyType
    }

    static member inline get_Codec () =
        codec {
            let! source = jreq "src" (fun x -> Some x.Source)
            and! destination = jreq "dest" (fun x -> Some x.Destination)
            and! messageBody = jreq "body" (fun x -> Some x.MessageBody)

            return
                {
                    Source = source
                    Destination = destination
                    MessageBody = messageBody
                }
        }
        |> ofObjCodec

    static member inline ToJson (x: Message<_>) =
        jobj [ "src" .= x.Source; "dest" .= x.Destination; "body" .= x.MessageBody ]

    static member inline OfJson json =
        match json with
        | JObject o ->
            monad {
                let! src = jget o "src"
                and! dest = jget o "dest"
                and! body = jget o "body"

                return
                    {
                        Source = src
                        Destination = dest
                        MessageBody = body
                    }
            }
        | x -> Decode.Fail.objExpected x
