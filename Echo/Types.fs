[<AutoOpen>]
module Echo.Types

open Types
open Fleece

type EchoMessage =
    {
        MessageId: MessageId
        Message: string
    }

    static member inline get_Codec () =
        codec {
            let! _type = jreq "type" (fun x -> Some "echo")
            and! messageId = jreq "msg_id" (fun x -> Some x.MessageId)
            and! message = jreq "echo" (fun x -> Some x.Message)

            return
                {
                    MessageId = messageId
                    Message = message
                }
        }
        |> ofObjCodec

type EchoReplyMessage =
    {
        MessageId: MessageId
        Message: string
        InReplyTo: MessageId
    }

    static member inline get_Codec () =
        codec {
            let! _type = jreq "type" (fun x -> Some "echo_ok")
            and! messageId = jreq "msg_id" (fun x -> Some x.MessageId)
            and! message = jreq "echo" (fun x -> Some x.Message)
            and! inReplyTo = jreq "in_reply_to" (fun x -> Some x.InReplyTo)

            return
                {
                    MessageId = messageId
                    Message = message
                    InReplyTo = inReplyTo
                }
        }
        |> ofObjCodec
