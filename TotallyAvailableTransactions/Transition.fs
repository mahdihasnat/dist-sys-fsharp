[<AutoOpen>]
module TotallyAvailableTransactions.Transition

open TotallyAvailableTransactions
open Types

let transition (node: Node) (action: Choice<Message<InputMessageBody>, unit>) : TransitionResult =
    transition {
        match action with
        | Choice2Of2 () -> return node
        | Choice1Of2 msg ->
            match msg.MessageBody with
            | InputMessageBody.Txn (messageId, transactionOperationInputs) -> return node
            | InputMessageBody.KVResponse kvResponseMessageBody -> return node
    }
