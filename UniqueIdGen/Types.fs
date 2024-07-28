[<AutoOpen>]
module UniqueIdGen.Types


open Types

open Fleece

type Node = {
    Info: InitialNodeInfo
    Counter: int
}

type GenerateMessageBody = {
    MessageId: MessageId
}
with
    static member get_Codec () =
        codec {
            let! _type = jreq "type" (fun x -> Some "generate")
            and! messageId = jreq "msg_id" (fun x -> Some x.MessageId)
            return {
                MessageId = messageId
            }
        }
        |> ofObjCodec

type GenerateReplyMessageBody = {
    MessageId: MessageId
    InReplyTo: MessageId
    Id: string
}
with
    static member get_Codec () =
        codec {
            let! _type = jreq "type" (fun x -> Some "generate_ok")
            and! messageId = jreq "msg_id" (fun x -> Some x.MessageId)
            and! inReplyTo = jreq "in_reply_to" (fun x -> Some x.InReplyTo)
            and! id = jreq "id" (fun x -> Some x.Id)
            return {
                MessageId = messageId
                InReplyTo = inReplyTo
                Id = id
            }
        }
        |> ofObjCodec