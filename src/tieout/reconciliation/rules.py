"""Tolerances and thresholds — see docs/RECONCILIATION_RULES.md.

Every value here is a named constant with a stated justification (CLAUDE.md
framework point 4: never a bare magic number). The engine reads these; so does
the simulator, which must perturb *beyond* them to produce breaks that
classify() will actually flag.
"""

from decimal import Decimal

QUANTITY_TOLERANCE = Decimal("0")

PRICE_TOLERANCE = Decimal("0.01")

MISSING_TRADE_TIMEOUT_S = 300
