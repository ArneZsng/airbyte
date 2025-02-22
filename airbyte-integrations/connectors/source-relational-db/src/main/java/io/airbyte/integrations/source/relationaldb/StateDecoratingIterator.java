/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.relationaldb;

import com.google.common.collect.AbstractIterator;
import io.airbyte.db.IncrementalUtils;
import io.airbyte.integrations.base.AirbyteStreamNameNamespacePair;
import io.airbyte.integrations.source.relationaldb.state.StateManager;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteStateMessage;
import io.airbyte.protocol.models.JsonSchemaPrimitive;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateDecoratingIterator extends AbstractIterator<AirbyteMessage> implements Iterator<AirbyteMessage> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StateDecoratingIterator.class);

  private final Iterator<AirbyteMessage> messageIterator;
  private final StateManager stateManager;
  private final AirbyteStreamNameNamespacePair pair;
  private final String cursorField;
  private final JsonSchemaPrimitive cursorType;
  private final int stateEmissionFrequency;

  private final String initialCursor;
  private String maxCursor;
  private boolean hasEmittedFinalState;

  // The intermediateStateMessage is set to the latest state message.
  // For every stateEmissionFrequency messages, emitIntermediateState is set to true and
  // the latest intermediateStateMessage will be emitted.
  private int totalRecordCount = 0;
  private boolean emitIntermediateState = false;
  private AirbyteMessage intermediateStateMessage = null;
  private boolean hasCaughtException = false;

  /**
   * @param stateManager Manager that maintains connector state
   * @param pair Stream Name and Namespace (e.g. public.users)
   * @param cursorField Path to the comparator field used to track the records read so far
   * @param initialCursor name of the initial cursor column
   * @param cursorType ENUM type of primitive values that can be used as a cursor for checkpointing
   * @param stateEmissionFrequency If larger than 0, intermediate states will be emitted for every
   *        stateEmissionFrequency records. Only emit intermediate states if the records are sorted by
   *        the cursor field.
   */
  public StateDecoratingIterator(final Iterator<AirbyteMessage> messageIterator,
                                 final StateManager stateManager,
                                 final AirbyteStreamNameNamespacePair pair,
                                 final String cursorField,
                                 final String initialCursor,
                                 final JsonSchemaPrimitive cursorType,
                                 final int stateEmissionFrequency) {
    this.messageIterator = messageIterator;
    this.stateManager = stateManager;
    this.pair = pair;
    this.cursorField = cursorField;
    this.cursorType = cursorType;
    this.initialCursor = initialCursor;
    this.maxCursor = initialCursor;
    this.stateEmissionFrequency = stateEmissionFrequency;
  }

  private String getCursorCandidate(final AirbyteMessage message) {
    final String cursorCandidate = message.getRecord().getData().get(cursorField).asText();
    return (cursorCandidate != null ? cursorCandidate.replaceAll("\u0000", "") : null);
  }

  /**
   * Computes the next record retrieved from Source stream. Emits StateMessage containing data of the
   * record that has been read so far
   *
   * <p>
   * If this method throws an exception, it will propagate outward to the {@code hasNext} or
   * {@code next} invocation that invoked this method. Any further attempts to use the iterator will
   * result in an {@link IllegalStateException}.
   * </p>
   *
   * @return {@link AirbyteStateMessage} containing information of the records read so far
   */
  @Override
  protected AirbyteMessage computeNext() {
    if (hasCaughtException) {
      // Mark iterator as done since the next call to messageIterator will result in an
      // IllegalArgumentException and resets exception caught state.
      // This occurs when the previous iteration emitted state so this iteration cycle will indicate
      // iteration is complete
      hasCaughtException = false;
      return endOfData();
    }

    if (messageIterator.hasNext()) {
      Optional<AirbyteMessage> optionalIntermediateMessage = getIntermediateMessage();
      if (optionalIntermediateMessage.isPresent()) {
        return optionalIntermediateMessage.get();
      }

      totalRecordCount++;
      // Use try-catch to catch Exception that could occur when connection to the database fails
      try {
        final AirbyteMessage message = messageIterator.next();
        if (message.getRecord().getData().hasNonNull(cursorField)) {
          final String cursorCandidate = getCursorCandidate(message);
          if (IncrementalUtils.compareCursors(maxCursor, cursorCandidate, cursorType) < 0) {
            if (stateEmissionFrequency > 0 && !Objects.equals(maxCursor, initialCursor) && messageIterator.hasNext()) {
              // Only emit an intermediate state when it is not the first or last record message,
              // because the last state message will be taken care of in a different branch.
              intermediateStateMessage = createStateMessage(false);
            }
            maxCursor = cursorCandidate;
          }
        }

        if (stateEmissionFrequency > 0 && totalRecordCount % stateEmissionFrequency == 0) {
          emitIntermediateState = true;
        }

        return message;
      } catch (final Exception e) {
        emitIntermediateState = true;
        hasCaughtException = true;
        LOGGER.error("Message iterator failed to read next record. {}", e.getMessage());
        optionalIntermediateMessage = getIntermediateMessage();
        return optionalIntermediateMessage.orElse(endOfData());
      }
    } else if (!hasEmittedFinalState) {
      return createStateMessage(true);
    } else {
      return endOfData();
    }
  }

  /**
   * Returns AirbyteStateMessage when in a ready state, a ready state means that it has satifies the
   * conditions of:
   * <p>
   * cursorField has changed (e.g. 08-22-2022 -> 08-23-2022) and there have been at least
   * stateEmissionFrequency number of records since the last emission
   * </p>
   *
   * @return AirbyteStateMessage if one exists, otherwise Optional indicating state was not ready to
   *         be emitted
   */
  protected final Optional<AirbyteMessage> getIntermediateMessage() {
    if (emitIntermediateState && intermediateStateMessage != null) {
      final AirbyteMessage message = intermediateStateMessage;
      intermediateStateMessage = null;
      emitIntermediateState = false;
      return Optional.of(message);
    }
    return Optional.empty();
  }

  /**
   * Creates AirbyteStateMessage while updating the cursor used to checkpoint the state of records
   * read up so far
   *
   * @param isFinalState marker for if the final state of the iterator has been reached
   * @return AirbyteMessage which includes information on state of records read so far
   */
  public AirbyteMessage createStateMessage(final boolean isFinalState) {
    final AirbyteStateMessage stateMessage = stateManager.updateAndEmit(pair, maxCursor);
    LOGGER.info("State Report: stream name: {}, original cursor field: {}, original cursor value {}, cursor field: {}, new cursor value: {}",
        pair,
        stateManager.getOriginalCursorField(pair).orElse(null),
        stateManager.getOriginalCursor(pair).orElse(null),
        stateManager.getCursorField(pair).orElse(null),
        stateManager.getCursor(pair).orElse(null));

    if (isFinalState) {
      hasEmittedFinalState = true;
      if (stateManager.getCursor(pair).isEmpty()) {
        LOGGER.warn("Cursor was for stream {} was null. This stream will replicate all records on the next run", pair);
      }
    }

    return new AirbyteMessage().withType(Type.STATE).withState(stateMessage);
  }

}
