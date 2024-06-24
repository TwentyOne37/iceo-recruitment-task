package com.example.stream

import cats.MonadError
import cats.data.EitherT
import cats.effect.{Ref, Resource}
import cats.effect.kernel.Async
import cats.effect.std.Queue
import fs2.Stream
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import skunk._

import scala.concurrent.duration.{DurationInt, FiniteDuration}

// All SQL queries inside the Queries object are correct and should not be changed
final class TransactionStream[F[_]](
  operationTimer: FiniteDuration,
  orders: Queue[F, OrderRow],
  session: Resource[F, Session[F]],
  transactionCounter: Ref[F, Int], // updated if long IO succeeds
  stateManager: StateManager[F],   // utility for state management
  maxConcurrency: Int
)(implicit F: Async[F], logger: Logger[F]) {

  def stream: Stream[F, Unit] = {
    Stream
      .fromQueueUnterminated(orders)
      .evalMap(order => processUpdate(order))
      .parEvalMap(maxConcurrency)(_ => F.unit)
  }

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled <- ngl, it's a tricky comment
  private def processUpdate(updatedOrder: OrderRow): F[Unit] = {
    F.uncancelable { _ =>
      PreparedQueries(session).use { queries =>
        for {
          maybeOrderState <- stateManager.getOrderState(updatedOrder.orderId, queries)
          _ <-
            maybeOrderState.fold(
              // Order not found initially, log and wait to retry
              for {
                _ <- logger.info(s"Order ${updatedOrder.orderId} not found. Retrying after 5 seconds...")
                _ <- F.sleep(5.seconds) // Wait for 5 seconds before retrying
                retriedOrderState <- stateManager.getOrderState(updatedOrder.orderId, queries)
                _ <- retriedOrderState.fold(
                       // Log error if order still not found after retry
                       logger.error(s"Order ${updatedOrder.orderId} still not found after retry.") *> F.unit
                     )(orderState =>
                       // Process the order if found after retry
                       if (shouldProcessUpdate(orderState, updatedOrder)) {
                         processTransaction(orderState, updatedOrder, queries)
                       } else {
                         logger.info(s"No processing needed for order ${updatedOrder.orderId} after retry.") *> F.unit
                       }
                     )
              } yield ()
            )(orderState =>
              // Process the order if found initially
              if (shouldProcessUpdate(orderState, updatedOrder)) {
                processTransaction(orderState, updatedOrder, queries)
              } else {
                logger.info(s"No processing needed for order ${updatedOrder.orderId}.") *> F.unit
              }
            )
        } yield ()
      }
    }
  }

  private def shouldProcessUpdate(orderState: OrderRow, updatedOrder: OrderRow): Boolean = {
    updatedOrder.filled <= orderState.total
  }

  private def processTransaction(
    orderState: OrderRow,
    updatedOrder: OrderRow,
    queries: PreparedQueries[F]
  ): F[Unit] = {
    val transaction = TransactionRow(state = orderState, updated = updatedOrder)
    val params      = updatedOrder.orderId *: updatedOrder.filled *: EmptyTuple

    for {
      transactionExists <- stateManager.transactionExists(params, queries)
      _ <- performLongRunningOperation(transaction, transactionExists).value.flatMap(
             handleOperationOutcome(_, queries, updatedOrder, transaction)
           )
    } yield ()
  }

  // represents some long running IO that can fail
  private def performLongRunningOperation(
    transaction: TransactionRow,
    transactionExists: Boolean
  ): EitherT[F, Throwable, Unit] = {
    EitherT.liftF[F, Throwable, Unit](
      F.sleep(operationTimer) *> // Simulate long-running operation
        stateManager.getSwitch.flatMap {
          case false =>
            if (transaction.amount > 0 || transactionExists) {
              transactionCounter
                .updateAndGet(_ + 1)
                .flatMap(count =>
                  logger.info(
                    s"Updated counter to $count by transaction with amount ${transaction.amount} for order ${transaction.orderId}!"
                  )
                ) *> F.unit
            } else F.unit
          case true =>
            // Raise an error if the switch is on (true), indicating a failure condition
            F.raiseError(new Exception("Long running IO failed!"))
        }
    )
  }

  private def handleOperationOutcome(
    operationOutcome: Either[Throwable, Unit],
    queries: PreparedQueries[F],
    updatedOrder: OrderRow,
    transaction: TransactionRow
  ): F[Unit] = {
    operationOutcome match {
      case Right(_) =>
        if (transaction.amount > 0)
          insertRecords(queries, updatedOrder, transaction)
        else F.unit

      case Left(th) =>
        logger.error(th)("Got error when performing long running operation!")
        MonadError[F, Throwable].raiseError[Unit](new RuntimeException("Long running operation failed", th))
    }
  }

  private def insertRecords(
    queries: PreparedQueries[F],
    updatedOrder: OrderRow,
    transaction: TransactionRow
  ): F[Unit] = {
    val params = updatedOrder.filled *: updatedOrder.orderId *: EmptyTuple
    for {
//      _ <- if (updatedOrder.filled == updatedOrder.total)
      _ <- queries.updateOrder.execute(params)
      _ <- queries.insertTransaction.execute(transaction)
    } yield ()
  }

  private def drainQueue(queue: Queue[F, OrderRow]): F[Unit] = {
    queue.tryTake.flatMap {
      case Some(order) =>
        processUpdate(order) >> drainQueue(queue)
      case None =>
        Async[F].unit // When no more items, complete the shutdown.
    }
  }

  // helper methods for testing
  def publish(update: OrderRow): F[Unit] = orders.offer(update)
  def getCounter: F[Int]                 = transactionCounter.get
  def setSwitch(value: Boolean): F[Unit] = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] =
    stateManager.addNewOrderState(order, insert)
  // helper methods for testing
}

object TransactionStream {

  def apply[F[_]: Async: Logger](
    operationTimer: FiniteDuration,
    session: Resource[F, Session[F]]
  ): Resource[F, TransactionStream[F]] = {
    for {
      counter      <- Resource.eval(Ref.of(0))
      stateManager <- Resource.eval(StateManager.apply[F])
      maxConcurrency = 3
      queue <- Resource.make(Queue.unbounded[F, OrderRow]) { queue =>
                 // Function to drain the queue on resource release
                 val stream = new TransactionStream[F](
                   operationTimer,
                   queue,
                   session,
                   counter,
                   stateManager,
                   maxConcurrency
                 )
                 stream.drainQueue(queue)
               }

    } yield new TransactionStream[F](
      operationTimer,
      queue,
      session,
      counter,
      stateManager,
      maxConcurrency
    )
  }
}
