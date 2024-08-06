[<AutoOpen>]
module BroadCast.Program

open System
open System.Threading
open FSharpPlus
open Fleece.SystemTextJson
open Microsoft.FSharp.Control
open Types
open Utilities
open BroadCast
open System.Threading.Tasks

let semaphore: SemaphoreSlim = new SemaphoreSlim(1)

let repeatSchedule (nodeRef: ref<Node>) : Task<unit> =
    task {
        while true do
            do! Task.Delay (TimeSpan.FromMilliseconds 10)
            do! semaphore.WaitAsync ()
            let (node', messages) = transition nodeRef.Value (Choice2Of2 ())
            nodeRef.Value <- node'
            messages
            |> List.iter (fun msg ->
                let json = toJsonText msg
                printfn $"{json}"
                eprintfn $"STDOUT: {json}"
            )
            semaphore.Release () |> ignore
    }


let rec processStdin (nodeRef: ref<Node>) : Task<unit> =
    task {
        let! str = Task.Run (fun _ -> Console.ReadLine ())
        if str = null then
            return ()
        else
            eprintfn $"STDIN: {str}"
            let msg =
                match ofJsonText<Message<InputMessageBody>> str with
                | Error e ->
                    failwithf $"Failed to parse input message: {e}"
                | Ok msg ->
                    msg
            do! semaphore.WaitAsync ()
            let (node', messages) = transition nodeRef.Value (Choice1Of2 msg)
            nodeRef.Value <- node'
            messages
            |> List.iter (fun msg ->
                let json = toJsonText msg
                printfn $"{json}"
                eprintfn $"STDOUT: {json}"
            )
            semaphore.Release () |> ignore
            return! processStdin nodeRef
    }

[<EntryPoint>]
let main _args =
    let nodeInfo = initNode ()
    let nodeRef : ref<Node> =
        ref
            {
                Info = nodeInfo
                Messages = Set.empty
                Neighbors = Set.empty
                MessageCounter = 0
                PendingAck = Map.empty
                NeighborAckedMessages = Map.empty
            }
    let task1 = processStdin nodeRef
    let async1 = task1 |> Async.AwaitTask
    let task2 = repeatSchedule nodeRef
    let async2 = task2 |> Async.AwaitTask
    [| async2; async1 |]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    0