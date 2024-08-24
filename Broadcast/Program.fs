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
    let semaphore: SemaphoreSlim = new SemaphoreSlim(1)
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
    let task1 = processStdin
                    (nodeRef, semaphore)
                    transition
    let async1 = task1 |> Async.AwaitTask
    let task2 = repeatSchedule
                    (TimeSpan.FromMilliseconds (float timerIntervalMilliseconds))
                    (nodeRef, semaphore)
                    (fun _node -> ())
                    (transition)
    let async2 = task2 |> Async.AwaitTask
    [| async2; async1 |]
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore

    0