[<AutoOpen>]
module BroadCast.Program

open System
open Fleece.SystemTextJson
open Types
open Utilities
open BroadCast



let rec processStdin (node: Node) : unit =

    let str = Console.ReadLine ()
    if str = null then
        ()
    else
        eprintfn $"STDIN: {str}"
        let msg =
            match ofJsonText<Message<InputMessageBody>> str with
            | Error e ->
                failwithf $"Failed to parse input message: {e}"
            | Ok msg ->
                msg

        let (node, messages) = transition node (Choice1Of2 msg)
        messages
        |> List.iter (fun msg ->
            let json = toJsonText msg
            printfn $"{json}"
            eprintfn $"STDOUT: {json}"
        )
        let (node, messages) = transition node (Choice2Of2 ())
        messages
        |> List.iter (fun msg ->
            let json = toJsonText msg
            printfn $"{json}"
            eprintfn $"STDOUT: {json}"
        )
        processStdin node

[<EntryPoint>]
let main _args =
    let nodeInfo = initNode ()
    processStdin {
        Info = nodeInfo
        Messages = Set.empty
        Neighbors = Set.empty
        MessageCounter = 0
        PendingAck = Map.empty
        NeighborAckedMessages = Map.empty
    }
    0