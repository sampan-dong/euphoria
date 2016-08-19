
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.triggers.Triggerable;

import java.io.Serializable;
import java.util.concurrent.ScheduledFuture;

/**
 * Schedules and fires registered triggers according to internal time
 */
public interface TriggerScheduler extends Serializable {

  /**
   * Fire specific trigger on given time.
   * Schedule the given trigger at the given stamp.
   * The trigger will be fired as close to the time as possible.
   * @return delayed result as a future or {@code null} when time already passed
   */
  ScheduledFuture<Void> scheduleAt(long stamp, WindowContext w, Triggerable trigger);

  long getCurrentTimestamp();

  /**
   * Cancel all scheduled tasks
   */
  void cancelAll();

  /**
   * Cancel all tasks scheduled for specified window
   */
  void cancel(WindowContext w);

  /**
   * Update the internal timestamp by processed element (optional operation).
   */
  default void updateProcessed(long stamp) {
    // nop
  }

  /**
   * Close all triggers and destroy the triggering.
   */
  void close();
  
}