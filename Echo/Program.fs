module Echo.Program

open Fleece.SystemTextJson
open System
open Types
open Utilities

let rec processStdin (messageId: int) : unit =

    let str = Console.ReadLine()
    if str = null then
        ()
    else
        eprintfn $"STDIN: {str}"
        let echoMessage =
            match ofJsonText<Message<EchoMessage>> str with
            | Error e ->
                failwith $"{e}"
            | Ok msg ->
                msg

        let replyMessageBody : EchoReplyMessage =
            {
                Message = echoMessage.MessageBody.Message
                InReplyTo = echoMessage.MessageBody.MessageId
                MessageId = MessageId messageId
            }
        let replyMessage : Message<EchoReplyMessage> = {
            Source = echoMessage.Destination
            Destination = echoMessage.Source
            MessageBody = replyMessageBody
        }
        printfn $"{toJsonText replyMessage}"
        eprintfn $"STDOUT: {toJsonText replyMessage}"
        processStdin (messageId + 1)

[<EntryPoint>]
let main _args =
    initNode () |> ignore
    processStdin (1)
    0