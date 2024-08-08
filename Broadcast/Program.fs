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
open Argu

let semaphore: SemaphoreSlim = new SemaphoreSlim(1)

let repeatSchedule (timerInterval: TimeSpan) (nodeRef: ref<Node>) : Task<unit> =
    let mutable lastAction = DateTimeOffset.Now
    task {
        while true do
            let now = DateTimeOffset.Now
            if now < lastAction + timerInterval then
                do! Task.Delay (lastAction + timerInterval - now)
            lastAction <- DateTimeOffset.Now

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

[<RequireQualifiedAccess>]
type Arguments=
    | [<MainCommand>] TimerIntervalMilliseconds of int

    interface IArgParserTemplate with
        member this.Usage =
            match this with
            | TimerIntervalMilliseconds _ -> "TimerIntervalMilliseconds <milliseconds>"

[<EntryPoint>]
let main args =
    let parser = ArgumentParser.Create<Arguments> ()
    eprintfn $"Arguments: {args}"
    let parseResults = parser.ParseCommandLine(inputs = args, raiseOnUsage = false, ignoreMissing = false, ignoreUnrecognized = false)
    let timerIntervalMilliseconds =
        parseResults.GetResult(Arguments.TimerIntervalMilliseconds)
    eprintfn $"TimerIntervalMilliseconds: {timerIntervalMilliseconds}"
    let nodeInfo = initNode ()
    let nodeRef : ref<Node> =
        ref
            {
                Info = nodeInfo
                Messages = Set.empty
                Neighbors = Set.empty
                MessageCounter = 0
                PendingAck = Map.empty
                TimedOutMessages = Map.empty
                NeighborAckedMessages = Map.empty
            }
    let task1 = processStdin nodeRef
    let async1 = task1 |> Async.AwaitTask
    let task2 = repeatSchedule (TimeSpan.FromMilliseconds (float timerIntervalMilliseconds)) nodeRef
    let async2 = task2 |> Async.AwaitTask
    [| async2; async1 |]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    0