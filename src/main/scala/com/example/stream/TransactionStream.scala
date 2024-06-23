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
  stateManager: StateManager[F],   // utility for state management
  maxConcurrency: Int
)(implicit F: Async[F], logger: Logger[F]) {

  def stream: Stream[F, Unit] = {
    Stream
      .fromQueueUnterminated(orders)
      .parEvalMap(maxConcurrency)(processUpdate)

  }

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled
  private def processUpdate(updatedOrder: OrderRow): F[Unit] = {
    F.uncancelable { _ =>
      PreparedQueries(session).use { queries =>
        for {
          maybeOrderState <- stateManager.getOrderState(updatedOrder.orderId)
          _ <- maybeOrderState.fold(logger.info(s"Order ${updatedOrder.orderId} not found.") *> F.unit)(orderState =>
                 processTransaction(orderState, updatedOrder, queries)
               )
        } yield ()
      }
    }
  }

  private def processTransaction(
    orderState: OrderRow,
    updatedOrder: OrderRow,
    queries: PreparedQueries[F]
  ): F[Unit] = {
    val transaction = TransactionRow(state = orderState, updated = updatedOrder)
    for {
      _                 <- logger.info(s"Found orderState: $orderState")
      transactionExists <- stateManager.transactionExists(transaction.id)
      _                 <- logger.info(s"Transaction: $transaction exists: $transactionExists")
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
      logger.info("Performing long IO...") *>
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
      case Right(_) if updatedOrder.filled == updatedOrder.total =>
        for {
          _                 <- logger.info("Order filled")
          transactionsState <- stateManager.getTransactions(updatedOrder.orderId)
          transactions = transaction :: transactionsState
          _ <- insertRecords(queries, updatedOrder, transactions)
        } yield ()

      case Right(_) =>
        logger.info("Order still in progress")
        stateManager.addNewTransaction(transaction)
        stateManager.updateOrderState(updatedOrder)
      case Left(th) =>
        logger.error(th)("Got error when performing long running operation!")
        MonadError[F, Throwable].raiseError[Unit](new RuntimeException("Long running operation failed", th))
    }
  }

  private def insertRecords(
    queries: PreparedQueries[F],
    updatedOrder: OrderRow,
    transactions: List[TransactionRow]
  ): F[Unit] = {
    val params = updatedOrder.filled *: updatedOrder.orderId *: EmptyTuple
    for {
      _ <- queries.insertOrder.execute(updatedOrder).void
      _ <- logger.info(s"Transactions: $transactions")
      _ <- transactions.traverse_(transaction => {
             logger.info(s"inserting transaction: $transaction") *>
               queries.insertTransaction.execute(transaction).void
           })
      // clean state
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
  def publish(update: OrderRow): F[Unit]                                          = orders.offer(update)
  def getCounter: F[Int]                                                          = transactionCounter.get
  def setSwitch(value: Boolean): F[Unit]                                          = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = stateManager.addNewOrderState(order)
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
                   stateManager,
                   2
                 )
                 stream.drainQueue(queue)
               }

    } yield new TransactionStream[F](
      operationTimer,
      queue,
      session,
      counter,
      stateManager,
      2
    )
  }
}
