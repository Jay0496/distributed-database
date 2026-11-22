"""gRPC auth interceptor."""

import contextlib
import hmac
import os

import grpc

TOKEN = os.environ.get("TOKEN", "")


def _get_token():
    """Resolve the current token, honoring env changes and test overrides."""
    env_val = os.environ.get("TOKEN", None)
    if env_val is not None:
        if env_val == "":
            return ""
        return env_val
    # Backward compat: honor a patched module global on node.server if present.
    try:
        import node.server as _server_mod

        val = getattr(_server_mod, "TOKEN", None)
        if val is not None:
            return val or ""
    except Exception:
        pass
    try:
        import server as _server_mod  # type: ignore

        val = getattr(_server_mod, "TOKEN", None)
        if val is not None:
            return val or ""
    except Exception:
        pass
    return globals().get("TOKEN", "") or ""


class AuthInterceptor(grpc.ServerInterceptor):
    """Unary server interceptor enforcing a shared bearer token.

    Auth is disabled when TOKEN is empty.
    """

    def intercept_service(self, continuation, handler_call_details):
        token = _get_token()
        if not token:
            return continuation(handler_call_details)

        provided = None
        metadata = getattr(handler_call_details, "invocation_metadata", None) or ()
        for key, value in metadata:
            if str(key).lower() == "authorization":
                provided = value
                break

        ok = False
        if isinstance(provided, str) and provided.startswith("Bearer "):
            candidate = provided[len("Bearer "):]
            try:
                ok = hmac.compare_digest(token, candidate)
            except Exception:
                ok = False
        elif isinstance(provided, (bytes, bytearray)):
            try:
                text = provided.decode("utf-8", "replace")
            except Exception:
                text = ""
            if text.startswith("Bearer "):
                candidate = text[len("Bearer "):]
                try:
                    ok = hmac.compare_digest(token, candidate)
                except Exception:
                    ok = False

        if not ok:
            def _abort(request, context):
                with contextlib.suppress(Exception):
                    context.abort(grpc.StatusCode.UNAUTHENTICATED, "unauthenticated")

            return grpc.unary_unary_rpc_method_handler(_abort)

        return continuation(handler_call_details)