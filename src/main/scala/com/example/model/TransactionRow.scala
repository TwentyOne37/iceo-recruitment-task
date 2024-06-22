package com.example.model

import java.time.Instant
import java.util.UUID

case class TransactionRow(
  id: UUID,
  orderId: String, // the same as OrderRow
  amount: BigDecimal,
  createdAt: Instant
)

object TransactionRow {

  def apply(state: OrderRow, updated: OrderRow): TransactionRow = {
    val amount =
      if (state.total == updated.filled)
        state.total - state.filled
      else if (state.filled > 0) updated.filled - state.filled
      else updated.filled

    TransactionRow(
      id = UUID.randomUUID(), // generate some id for our transaction
      orderId = state.orderId,
      amount = amount,
      createdAt = Instant.now()
    )
  }
}
