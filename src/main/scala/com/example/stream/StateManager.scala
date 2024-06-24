package com.example.stream

import cats.effect.Async
import com.example.model.OrderRow
import com.example.persistence.PreparedQueries
import skunk.{*:, PreparedCommand}
import cats.syntax.all._
import fs2.concurrent.SignallingRef
import scodec.compat.EmptyTuple

//Utility for managing placed order state
//This can be used by other components, for example a stream that performs order placement will use add method
final class StateManager[F[_]: Async](ioSwitch: SignallingRef[F, Boolean]) {

  def getOrderState(orderId: String, queries: PreparedQueries[F]): F[Option[OrderRow]] = {
    queries.getOrder.option(orderId)
  }

  def addNewOrderState(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = {
    insert.execute(order).void
  }

  def transactionExists(params: String *: BigDecimal *: EmptyTuple, queries: PreparedQueries[F]): F[Boolean] =
    queries.checkTransactionExistence.unique(params)

  def getSwitch: F[Boolean]              = ioSwitch.get
  def setSwitch(value: Boolean): F[Unit] = ioSwitch.set(value)
}

object StateManager {

  def apply[F[_]: Async]: F[StateManager[F]] = for {
    ioSwitch <- SignallingRef.of(false)
  } yield new StateManager[F](ioSwitch)

}
