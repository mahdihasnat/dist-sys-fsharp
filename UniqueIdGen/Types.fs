[<AutoOpen>]
module UniqueIdGen.Types


open Types

open Fleece

type Node = { Info: InitialNodeInfo; Counter: int }

type GenerateMessageBody =
    {
        MessageId: MessageId
    }

    static member get_Codec () =
        codec {
            let! _type = jreq "type" (fun x -> Some "generate")
            and! messageId = jreq "msg_id" (fun x -> Some x.MessageId)
            return { MessageId = messageId }
        }
        |> ofObjCodec

type GenerateReplyMessageBody =
    {
        InReplyTo: MessageId
        Id: string
    }

    static member get_Codec () =
        codec {
            let! _type = jreq "type" (fun x -> Some "generate_ok")
            and! inReplyTo = jreq "in_reply_to" (fun x -> Some x.InReplyTo)
            and! id = jreq "id" (fun x -> Some x.Id)
            return { InReplyTo = inReplyTo; Id = id }
        }
        |> ofObjCodec
