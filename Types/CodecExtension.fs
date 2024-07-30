[<AutoOpen>]
module Types.CodecExtension
let QQQ<'T> = failwithf "wow"

[<AutoOpen>]
module Codec =
    open Fleece
    let inline isomorph (enc: 'U -> 'T) (dec: 'T -> 'U) (codec1: Codec<'a,'a,'T,'T>) : Codec<'a,'a,'U,'U> when 'a :> IEncoding and 'a: (new : unit -> 'a) =
        let dec1 : 'a -> ParseResult<'T> = Codec.decode codec1
        let enc1 : 'T -> 'a = Codec.encode codec1
        let dec2 : 'a -> ParseResult<'U> = dec1 >> (Result.map dec)
        let enc2 : 'U -> 'a = enc >> enc1
        Codec.create dec2 enc2
