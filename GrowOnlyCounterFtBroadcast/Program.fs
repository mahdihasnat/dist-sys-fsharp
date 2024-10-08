[<AutoOpen>]
module GrowOnlyCounter.Program

open System
open System.Threading
open FSharpPlus
open Fleece.SystemTextJson
open Microsoft.FSharp.Control
open Types
open Utilities
open GrowOnlyCounter
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
                Transactions = Set.empty
                Neighbors =
                    generateGraph nodeInfo.ClusterNodeIds
                    |> Map.tryFind nodeInfo.NodeId
                    |> Option.defaultValue Set.empty
                MessageCounter = 0
                PendingAck = Map.empty
                TimedOutMessages = Map.empty
                NeighborAckedMessages = Map.empty
            }

    let task1 = processStdin (nodeRef, semaphore) transition
    let async1 = task1 |> Async.AwaitTask

    let task2 =
        repeatSchedule (TimeSpan.FromMilliseconds 100) (nodeRef, semaphore) (fun _ -> ()) transition

    let async2 = task2 |> Async.AwaitTask
    [| async2; async1 |] |> Async.Parallel |> Async.RunSynchronously |> ignore

    0
