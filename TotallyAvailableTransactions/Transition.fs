[<AutoOpen>]
module TotallyAvailableTransactions.Transition

open FSharpPlus.Data
open TotallyAvailableTransactions
open Types

let transition (node: Node) (action: Choice<Message<InputMessageBody>, unit>) : TransitionResult =
    transition {
        match action with
        | Choice2Of2 () -> return node
        | Choice1Of2 msg ->
            match msg.MessageBody with
            | InputMessageBody.Txn (messageId, transactionOperationInputs) ->

                let response, storage =
                    transactionOperationInputs
                    |> NonEmptyList.toList
                    |> List.mapFold
                        (fun storage transactionOperationInput ->
                            match transactionOperationInput with
                            | TransactionOperationInput.Read key -> TransactionOperationOutput.Read (key, Map.tryFind key node.Storage), storage
                            | TransactionOperationInput.Write (key, value) ->
                                TransactionOperationOutput.Write (key, value), Map.add key value storage)
                        node.Storage
                    |> fun (out, storage) -> NonEmptyList.ofList out, storage

                yield
                    {
                        Source = node.Info.NodeId
                        Destination = msg.Source
                        MessageBody = OutputMessageBody.TxnAck (messageId, response)
                    }

                return { node with Storage = storage }
            | InputMessageBody.KVResponse kvResponseMessageBody -> return node
    }
