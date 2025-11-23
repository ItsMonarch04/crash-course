# Threat model

ccdb's client, peer, admin, and metrics protocols are unauthenticated and
unencrypted. A party that can reach one of those listeners can impersonate a
peer, read data, or submit mutations. ccdb does not provide authentication,
authorization, confidentiality, or protection against an active network
attacker.

The supported deployment boundary is one trusted host or a private network
whose access controls the operator already manages. Listeners resolve to
loopback addresses by default. A non-loopback client, peer, or metrics
listener requires the explicit `--i-know-this-is-unauthenticated` opt-in; that
flag acknowledges exposure but does not authenticate a connection.

This project deliberately does not add a shared-secret MAC. Correctly adding
one requires reviewed cryptography, secure key distribution, rotation, and
replay protection. Those requirements are outside this crash-course lab's
current product boundary.
