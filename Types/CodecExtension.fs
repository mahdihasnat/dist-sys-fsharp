[<AutoOpen>]
module Types.CodecExtension

open Fleece
open Fleece.Internals
open FSharpPlus

let inline QQQ<'T> : 'T = failwithf "wow"

[<AutoOpen>]
module Codec =
    let inline isomorph (enc: 'U -> 'T) (dec: 'T -> 'U) (codec1: Codec<'a,'a,'T,'T>) : Codec<'a,'a,'U,'U> when 'a :> IEncoding and 'a: (new : unit -> 'a) =
        let dec1 : 'a -> ParseResult<'T> = Codec.decode codec1
        let enc1 : 'T -> 'a = Codec.encode codec1
        let dec2 : 'a -> ParseResult<'U> = dec1 >> (Result.map dec)
        let enc2 : 'U -> 'a = enc >> enc1
        Codec.create dec2 enc2

[<AutoOpen>]
module Operators =
    let inline jreqAlways (name: string) (getter: 'T -> 'param) : Codec<PropertyList<^Encoding>, PropertyList<^Encoding>, 'param, 'T> =
        jreq name (getter >> Option.Some)

type Variant<'Variant1, 'Variant2, ^Encoding
    when ^Encoding :> IEncoding
    and ^Encoding : (new : unit -> ^Encoding)
    and (GetCodec or 'Variant1) : (static member GetCodec: 'Variant1 * GetCodec * GetCodec * OpCodec -> Codec<^Encoding, 'Variant1>)
    and (GetCodec or 'Variant2) : (static member GetCodec: 'Variant2 * GetCodec * GetCodec * OpCodec -> Codec<^Encoding, 'Variant2>)
    > =
    | Variant1Of2 of 'Variant1
    | Variant2Of2 of 'Variant2
with
    static member inline get_Codec() =
        let codec1 = defaultCodec<_, 'Variant1>
        let codec2 = defaultCodec<_, 'Variant2>
        let encoder =
            function
                | Variant1Of2 variant1 -> variant1 |> (Codec.encode codec1)
                | Variant2Of2 variant2 -> variant2 |> (Codec.encode codec2)
        let decoder1 =
            (Codec.decode codec1) >> (Result.map Variant1Of2)
        let decoder2 =
            (Codec.decode codec2) >> (Result.map Variant2Of2)
        let decoder = decoder1 <|> decoder2
        Codec.create (decoder) (encoder)

type Variant<'Variant1, 'Variant2, 'Variant3, ^Encoding
    when ^Encoding :> IEncoding
    and ^Encoding : (new : unit -> ^Encoding)
    and (GetCodec or 'Variant1) : (static member GetCodec: 'Variant1 * GetCodec * GetCodec * OpCodec -> Codec<^Encoding, 'Variant1>)
    and (GetCodec or 'Variant2) : (static member GetCodec: 'Variant2 * GetCodec * GetCodec * OpCodec -> Codec<^Encoding, 'Variant2>)
    and (GetCodec or 'Variant3) : (static member GetCodec: 'Variant3 * GetCodec * GetCodec * OpCodec -> Codec<^Encoding, 'Variant3>)
    > =
    | Variant1Of3 of 'Variant1
    | Variant2Of3 of 'Variant2
    | Variant3Of3 of 'Variant3
with
    static member inline get_Codec() =
        let codec1 = defaultCodec<_, 'Variant1>
        let codec2 = defaultCodec<_, 'Variant2>
        let codec3 = defaultCodec<_, 'Variant3>
        let encoder =
            function
                | Variant1Of3 variant1 -> variant1 |> (Codec.encode codec1)
                | Variant2Of3 variant2 -> variant2 |> (Codec.encode codec2)
                | Variant3Of3 variant3 -> variant3 |> (Codec.encode codec3)
        let decoder1 =
            (Codec.decode codec1) >> (Result.map Variant1Of3)
        let decoder2 =
            (Codec.decode codec2) >> (Result.map Variant2Of3)
        let decoder3 =
            (Codec.decode codec3) >> (Result.map Variant3Of3)
        let decoder = decoder1 <|> decoder2 <|> decoder3
        Codec.create (decoder) (encoder)