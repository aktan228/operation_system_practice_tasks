"""A small arithmetic interpreter: no eval(), calls, variables or attributes."""
import ast
import math
import operator

OPS = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
       ast.Div: operator.truediv, ast.FloorDiv: operator.floordiv,
       ast.Mod: operator.mod, ast.Pow: operator.pow}


def calculate(expression):
    if not isinstance(expression, str) or not expression or len(expression) > 256:
        raise ValueError("expression must contain 1..256 characters")
    tree = ast.parse(expression, mode="eval")
    if sum(1 for _ in ast.walk(tree)) > 64:
        raise ValueError("expression too complex")

    def visit(node):
        if isinstance(node, ast.Constant) and type(node.value) in (int, float):
            value = node.value
        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            value = visit(node.operand)
            value = value if isinstance(node.op, ast.UAdd) else -value
        elif isinstance(node, ast.BinOp) and type(node.op) in OPS:
            left, right = visit(node.left), visit(node.right)
            if isinstance(node.op, ast.Pow) and abs(right) > 12:
                raise ValueError("absolute exponent must be <= 12")
            value = OPS[type(node.op)](left, right)
        else:
            raise ValueError("only numbers and arithmetic operators are allowed")
        if type(value) not in (int, float) or not math.isfinite(value) or abs(value) > 1e100:
            raise ValueError("result is outside the allowed range")
        return value

    return visit(tree.body)


def response(request):
    request_id = request.get("id") if isinstance(request, dict) else None
    try:
        if not isinstance(request, dict) or not isinstance(request_id, str):
            raise ValueError("request must contain a string id")
        return {"id": request_id, "ok": True, "result": calculate(request.get("expression"))}
    except (ValueError, SyntaxError, ArithmeticError, TypeError, RecursionError) as error:
        return {"id": request_id, "ok": False, "error": str(error)}
