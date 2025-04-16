package com.vincenzogulisano.javapythoncommunicator;

public enum PolicyBarrier {
    WELOB, // Wallclock, Event time, and Latency OBlivious
    ELOB, // Event time, and Latency OBlivious
    LOB, // Latency OBlivious
    WELAW;// Wallclock, Event time, and Latency Aware
}
