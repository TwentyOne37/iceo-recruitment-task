package com.example.stream

import cats.effect.{Async, Ref}
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import skunk.PreparedCommand
import cats.syntax.all._
import fs2.concurrent.SignallingRef

import java.util.UUID

//Utility for managing placed order state
//This can be used by other components, for example a stream that performs order placement will use add method
final class StateManager[F[_]: Async](ioSwitch: SignallingRef[F, Boolean]) {

  def getOrderState(orderId: String, queries: PreparedQueries[F]): F[Option[OrderRow]] = {
    queries.getOrder.option(orderId)
  }

  def addNewOrderState(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = {
    insert.execute(order).void
  }

  def updateOrderState(
    order: OrderRow,
    queries: PreparedQueries[F],
    update: PreparedCommand[F, OrderRow]
  ): F[Unit] =
    getOrderState(order.orderId, queries).map {
      case Some(_) => update.execute(order)
      case None    => Async[F].unit
    }

  def transactionExists(orderId: String, queries: PreparedQueries[F]): F[Boolean] =
    queries.checkTransactionExistence.unique(orderId)

  def getSwitch: F[Boolean]              = ioSwitch.get
  def setSwitch(value: Boolean): F[Unit] = ioSwitch.set(value)
}

object StateManager {

  def apply[F[_]: Async]: F[StateManager[F]] = for {
    ioSwitch <- SignallingRef.of(false)
  } yield new StateManager[F](ioSwitch)

}
