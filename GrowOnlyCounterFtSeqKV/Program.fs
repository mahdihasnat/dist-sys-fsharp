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

    let semaphore: SemaphoreSlim = new SemaphoreSlim(1)
    let nodeInfo = initNode ()
    let nodeRef : ref<Node> =
        ref
            {
                Info = nodeInfo
                NextMessageId = 0
                ValueCache = Value 0
                OnSeqKVReadOkHandlers = Map.empty
                OnSeqKVReadKeyDoesNotExistHandlers = Map.empty
                OnSeqKVCompareAndSwapOkHandlers = Map.empty
                OnSeqKVCompareAndSwapPreconditionFailedHandlers = Map.empty
            }
    let task1 = processStdin
                    (nodeRef, semaphore)
                    transition

    let task2 = repeatSchedule
                    (TimeSpan.FromMilliseconds 2000)
                    (nodeRef, semaphore)
                    (fun _node -> ())
                    transition
    [| task1; task2 |]
    |> Array.map Async.AwaitTask
    |> Async.Parallel
    |> Async.RunSynchronously
    |> ignore
    0