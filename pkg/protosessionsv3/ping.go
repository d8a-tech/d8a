package protosessionsv3

// TODO: Potential flaw in design!!!! When batching hits, if there's a ping in between, it may advance the lastHitProcessingTime
// on the timing wheel and cause disruptions in processing. Validate, that it's not the case
