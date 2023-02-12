import signal


def threads_interrupt_initiator():
    """
    Each pool process will execute this as part of its
    initialization.
    Use this to keep safe for multiprocessing...and gracefully interrupt by keyboard
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    