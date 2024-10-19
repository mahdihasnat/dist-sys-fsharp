[<AutoOpen>]
module GrowOnlyCounter.Program

open System
open System.Threading
open FSharpPlus
open FSharpPlus.Control
open Fleece.SystemTextJson
open Microsoft.FSharp.Control
open Types
open Utilities
open GrowOnlyCounter
open System.Threading.Tasks

open Utilities
open Fleece.SystemTextJson
open Fleece
open Fleece.CodecInterfaceExtensions
open Fleece.Codecs
open Fleece.Codec
open Fleece.Internals


[<EntryPoint>]
let main args =

    let semaphore: SemaphoreSlim = new SemaphoreSlim (1)
    let nodeInfo = initNode ()

    let nodeRef: ref<Node> = ref { Info = nodeInfo; Crdt = Map.empty }

    let task1 = processStdin (nodeRef, semaphore) transition
    let async1 = task1 |> Async.AwaitTask

    let task2 =
        repeatSchedule (TimeSpan.FromSeconds 1) (nodeRef, semaphore) (fun _ -> ()) transition

    let async2 = task2 |> Async.AwaitTask
    [| async2; async1 |] |> Async.Parallel |> Async.RunSynchronously |> ignore

    0
