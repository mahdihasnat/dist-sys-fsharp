﻿[<AutoOpen>]
module Kafka.Program

open System
open System.Threading
open FSharpPlus
open Fleece.SystemTextJson
open Microsoft.FSharp.Control
open Types
open Utilities
open Kafka
open System.Threading.Tasks

open Utilities

[<EntryPoint>]
let main args =
    let semaphore: SemaphoreSlim = new SemaphoreSlim (1)
    let nodeInfo = initNode ()

    let nodeRef: ref<Node> =
        ref
            {
                Info = nodeInfo
                NextMessageId = 0

                CachedMessages = Map.empty
                CurrentLogFetchingOperations = Map.empty

                OnKVReadOkHandlers = Map.empty
                OnKVWriteOkHandlers = Map.empty
                OnKVErrorKeyDoesNotExistHandlers = Map.empty
                OnKVCompareAndSwapOkHandlers = Map.empty
                OnKVErrorPreconditionFailedHandlers = Map.empty
            }

    let task1 = processStdin (nodeRef, semaphore) transitionOuter
    let async1 = task1 |> Async.AwaitTask
    // let task2 = repeatSchedule
    //                 (TimeSpan.FromMilliseconds 100)
    //                 (nodeRef, semaphore)
    //                 (fun _ -> ())
    //                 transition
    // let async2 = task2 |> Async.AwaitTask
    [| async1 |] |> Async.Parallel |> Async.RunSynchronously |> ignore

    0
