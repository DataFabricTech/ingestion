"""
Custom pylint plugin to catch `print` calls
"""
from typing import TYPE_CHECKING

from astroid import nodes
from pylint.checkers import BaseChecker
from pylint.checkers.utils import only_required_for_messages

if TYPE_CHECKING:
    from pylint.lint import PyLinter


class PrintChecker(BaseChecker):
    """
    Check for any print statement in the code
    """

    name = "no_print_allowed"
    _symbol = "print-call"
    msgs = {
        "W5001": (
            "Used builtin function %s",
            _symbol,
            "Print can make us lose traceability, use logging instead",
        )
    }

    @only_required_for_messages("print-call")
    def visit_call(self, node: nodes.Call) -> None:
        if isinstance(node.func, nodes.Name) and node.func.name == "print":
            self.add_message(self._symbol, node=node)


def register(linter: "PyLinter") -> None:
    """
    This required method auto registers the checker during initialization.
    :param linter: The linter to register the checker to.
    """
    linter.register_checker(PrintChecker(linter))
