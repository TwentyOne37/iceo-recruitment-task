package com.example.persistence

import cats.effect.Resource
import com.example.model.{OrderRow, TransactionRow}
import skunk._

final case class PreparedQueries[F[_]](
  xa: Transaction[F],
  insertOrder: PreparedCommand[F, OrderRow],
  getOrder: PreparedQuery[F, String, OrderRow],
  getAllOrders: PreparedQuery[F, Void, OrderRow],
  updateOrder: PreparedCommand[F, BigDecimal *: String *: EmptyTuple],
  insertTransaction: PreparedCommand[F, TransactionRow]
)

object PreparedQueries {

  def apply[F[_]](sessionR: Resource[F, Session[F]]): Resource[F, PreparedQueries[F]] = {
    sessionR.flatMap { session =>
      for {
        xa          <- session.transaction
        insertO     <- session.prepareR(Queries.insertOrder)
        insertT     <- session.prepareR(Queries.insertTransaction)
        getOrder    <- session.prepareR(Queries.getOrder)
        getOrders   <- session.prepareR(Queries.getAllOrders)
        updateOrder <- session.prepareR(Queries.updateOrder)
      } yield PreparedQueries(
        xa = xa,
        insertOrder = insertO,
        insertTransaction = insertT,
        getOrder = getOrder,
        getAllOrders = getOrders,
        updateOrder = updateOrder
      )
    }
  }
}
