module Echo.Program

open Fleece.SystemTextJson
open System
open Types

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
    // read line
    let strLine = Console.ReadLine ()
    if strLine = null then
        failwith "No input"
    eprintfn $"STDIN: {strLine}"
    let initMessage =
        match ofJsonText<Message<InitMessage>> strLine with
        | Error e ->
            failwith $"{e}"
        | Ok msg ->
            msg
    let initReply : InitReplyMessage =
        {
            InReplyTo = initMessage.MessageBody.MessageId
        }
    let replyMessage : Message<InitReplyMessage> =
        {
            Source = initMessage.MessageBody.NodeId
            Destination = initMessage.Source
            MessageBody = initReply
        }
    printfn $"{toJsonText replyMessage}"
    eprintfn $"STDOUT: {toJsonText replyMessage}"
    processStdin (1)
    0