package com.example.stream

import cats.{Monad, MonadError}
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

import scala.concurrent.duration.FiniteDuration

// All SQL queries inside the Queries object are correct and should not be changed
final class TransactionStream[F[_]](
  operationTimer: FiniteDuration,
  orders: Queue[F, OrderRow],
  session: Resource[F, Session[F]],
  transactionCounter: Ref[F, Int], // updated if long IO succeeds
  stateManager: StateManager[F]    // utility for state management
)(implicit F: Async[F], logger: Logger[F]) {

  def stream: Stream[F, Unit] = {
    Stream
      .fromQueueUnterminated(orders)
      .evalMap(processUpdate)

  }

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled
  private def processUpdate(updatedOrder: OrderRow): F[Unit] = {
    F.uncancelable { _ =>
      PreparedQueries(session).use { queries =>
        for {
          // Get the current known order state.
          state <- stateManager.getOrderState(updatedOrder, queries)
          transaction = TransactionRow(state = state, updated = updatedOrder)
          transactionExists <- stateManager.transactionExists(transaction, queries)

          // First, perform the long-running operation and handle failure.
          _ <- performLongRunningOperation(transaction, transactionExists).value.flatMap {
                 case Right(_) => Monad[F].unit // Continue if successful.
                 case Left(th) =>
                   logger.error(th)("Got error when performing long running operation!")
                   MonadError[F, Throwable].raiseError[Unit](new RuntimeException("Long running operation failed", th))
               }

          // Parameters for the order update.
          params = updatedOrder.filled *: updatedOrder.orderId *: EmptyTuple

          // Update order with params only if the previous step was successful.
          _ <- queries.updateOrder.execute(params)

          // Insert the transaction only if all previous steps were successful.
          _ <- if (updatedOrder.filled != 0) queries.insertTransaction.execute(transaction) else F.unit

        } yield ()
      }
    }
  }

  private def drainQueue(queue: Queue[F, OrderRow]): F[Unit] = {
    queue.tryTake.flatMap {
      case Some(order) =>
        processUpdate(order) >> drainQueue(queue)
      case None =>
        Async[F].unit // When no more items, complete the shutdown.
    }
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

  // helper methods for testing
  def publish(update: OrderRow): F[Unit]                                          = orders.offer(update)
  def getCounter: F[Int]                                                          = transactionCounter.get
  def setSwitch(value: Boolean): F[Unit]                                          = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = stateManager.add(order, insert)
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
      queue <- Resource.make(Queue.unbounded[F, OrderRow]) { queue =>
                 // Function to drain the queue on resource release
                 val stream = new TransactionStream[F](
                   operationTimer,
                   queue,
                   session,
                   counter,
                   stateManager
                 )
                 stream.drainQueue(queue)
               }

    } yield new TransactionStream[F](
      operationTimer,
      queue,
      session,
      counter,
      stateManager
    )
  }
}
