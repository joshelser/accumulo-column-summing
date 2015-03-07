Are you saying that if I first set up an iterator that took my key/value pairs like,

    000200001ccaac30 meta:size []    1807
    000200001ccaac30 meta:source []    data2
    000200001cdaac30 meta:filename []    doc02985453
    000200001cdaac30 meta:size []    656
    000200001cdaac30 meta:source []    data2
    000200001cfaac30 meta:filename []    doc04484522
    000200001cfaac30 meta:size []    565
    000200001cfaac30 meta:source []    data2
    000200001dcaac30 meta:filename []    doc03342958

And emitted something like,

    0 meta:size [] 1807
    0 meta:size [] 656
    0 meta:size [] 565

And then applied a SummingCombiner at a lower priority than that iterator, then... it should work, right?
