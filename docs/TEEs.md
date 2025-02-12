# How does Picachv work in tandem with TEEs?

As mentioned in the paper, the end-to-end security guarantees of Picachv hinges upon the existence of TEEs where on-premise data analytics is not possible due to practical reasons like lack of computational resources. By launching a remote attestation session with the TEE that hosts Picachv, users are assured that the verified Picachv indeed works in place.
