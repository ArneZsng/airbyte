/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.workers.temporal.TemporalActivityStubInterceptor.ActivityStubFunction;
import io.airbyte.workers.temporal.exception.RetryableException;
import io.airbyte.workers.temporal.stubs.ErrorTestWorkflowImpl;
import io.airbyte.workers.temporal.stubs.InvalidTestWorkflowImpl;
import io.airbyte.workers.temporal.stubs.TestActivity;
import io.airbyte.workers.temporal.stubs.ValidTestWorkflowImpl;
import io.micronaut.context.BeanRegistration;
import io.micronaut.inject.BeanIdentifier;
import io.temporal.activity.ActivityOptions;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link TemporalActivityStubInterceptor} class.
 */
class TemporalActivityStubInterceptorTest {

  private static final String ACTIVITY_OPTIONS = "activityOptions";

  @Test
  void testExecutionOfValidWorkflowWithActivities() throws Exception {
    final ActivityOptions activityOptions = mock(ActivityOptions.class);
    final TestActivity testActivity = mock(TestActivity.class);

    final BeanIdentifier beanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration beanRegistration = mock(BeanRegistration.class);
    when(beanIdentifier.getName()).thenReturn(ACTIVITY_OPTIONS);
    when(beanRegistration.getIdentifier()).thenReturn(beanIdentifier);
    when(beanRegistration.getBean()).thenReturn(activityOptions);

    final TemporalActivityStubInterceptor interceptor = new TemporalActivityStubInterceptor(ValidTestWorkflowImpl.class, List.of(beanRegistration));
    interceptor.setActivityStubGenerator((c, a) -> testActivity);

    final ValidTestWorkflowImpl validTestWorklowImpl = new ValidTestWorkflowImpl();
    final Callable<Void> callable = () -> {
      validTestWorklowImpl.run();
      return null;
    };

    interceptor.execute(validTestWorklowImpl, callable);
    Assertions.assertTrue(validTestWorklowImpl.isHasRun());
  }

  @Test
  void testExecutionOfValidWorkflowWithActivitiesThatThrows() throws Exception {
    final ActivityOptions activityOptions = mock(ActivityOptions.class);
    final TestActivity testActivity = mock(TestActivity.class);

    final BeanIdentifier beanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration beanRegistration = mock(BeanRegistration.class);
    when(beanIdentifier.getName()).thenReturn(ACTIVITY_OPTIONS);
    when(beanRegistration.getIdentifier()).thenReturn(beanIdentifier);
    when(beanRegistration.getBean()).thenReturn(activityOptions);

    final TemporalActivityStubInterceptor interceptor = new TemporalActivityStubInterceptor(ErrorTestWorkflowImpl.class, List.of(beanRegistration));
    interceptor.setActivityStubGenerator((c, a) -> testActivity);

    final ErrorTestWorkflowImpl errorTestWorkflowImpl = new ErrorTestWorkflowImpl();
    final Callable<Void> callable = () -> {
      errorTestWorkflowImpl.run();
      return null;
    };

    Assertions.assertThrows(RetryableException.class, () -> {
      interceptor.execute(errorTestWorkflowImpl, callable);
    });
  }

  @Test
  void testActivityStubsAreOnlyInitializedOnce() throws Exception {
    final AtomicInteger activityStubInitializationCounter = new AtomicInteger(0);
    final ActivityOptions activityOptions = mock(ActivityOptions.class);
    final TestActivity testActivity = mock(TestActivity.class);
    final ActivityStubFunction<Class<?>, ActivityOptions, Object> activityStubFunction = (c, a) -> {
      activityStubInitializationCounter.incrementAndGet();
      return testActivity;
    };

    final BeanIdentifier beanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration beanRegistration = mock(BeanRegistration.class);
    when(beanIdentifier.getName()).thenReturn(ACTIVITY_OPTIONS);
    when(beanRegistration.getIdentifier()).thenReturn(beanIdentifier);
    when(beanRegistration.getBean()).thenReturn(activityOptions);

    final TemporalActivityStubInterceptor interceptor = new TemporalActivityStubInterceptor(ValidTestWorkflowImpl.class, List.of(beanRegistration));
    interceptor.setActivityStubGenerator(activityStubFunction);

    final ValidTestWorkflowImpl validTestWorklowImpl = new ValidTestWorkflowImpl();
    final Callable<Void> callable = () -> {
      validTestWorklowImpl.run();
      return null;
    };
    interceptor.execute(validTestWorklowImpl, callable);
    interceptor.execute(validTestWorklowImpl, callable);
    interceptor.execute(validTestWorklowImpl, callable);
    interceptor.execute(validTestWorklowImpl, callable);

    Assertions.assertEquals(1, activityStubInitializationCounter.get());
  }

  @Test
  void testExecutionOfInvalidWorkflowWithActivityWithMissingActivityOptions() throws Exception {
    final ActivityOptions activityOptions = mock(ActivityOptions.class);
    final TestActivity testActivity = mock(TestActivity.class);

    final BeanIdentifier beanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration beanRegistration = mock(BeanRegistration.class);
    when(beanIdentifier.getName()).thenReturn(ACTIVITY_OPTIONS);
    when(beanRegistration.getIdentifier()).thenReturn(beanIdentifier);
    when(beanRegistration.getBean()).thenReturn(activityOptions);

    final TemporalActivityStubInterceptor interceptor = new TemporalActivityStubInterceptor(InvalidTestWorkflowImpl.class, List.of(beanRegistration));
    interceptor.setActivityStubGenerator((c, a) -> testActivity);

    final InvalidTestWorkflowImpl invalidTestWorklowImpl = new InvalidTestWorkflowImpl();
    final Callable<Void> callable = () -> {
      invalidTestWorklowImpl.run();
      return null;
    };

    final RuntimeException exception = Assertions.assertThrows(RuntimeException.class, () -> {
      interceptor.execute(invalidTestWorklowImpl, callable);
    });
    Assertions.assertEquals(IllegalStateException.class, exception.getCause().getClass());
  }

  @Test
  void testInvocationTargetExceptionWithError() {
    final Error target = new IllegalAccessError();
    final InvocationTargetException e = new InvocationTargetException(target);
    Assertions.assertThrows(IllegalAccessError.class, () -> {
      TemporalActivityStubInterceptor.handleInvocationTargetException(e);
    });
  }

  @Test
  void testInvocationTargetExceptionWithRuntimeException() {
    final RuntimeException target = new NullPointerException();
    final InvocationTargetException e = new InvocationTargetException(target);
    Assertions.assertThrows(NullPointerException.class, () -> {
      TemporalActivityStubInterceptor.handleInvocationTargetException(e);
    });
  }

  @Test
  void testInvocationTargetExceptionWithCheckedException() {
    final Exception target = new RetryableException(new NullPointerException("test"));
    final InvocationTargetException e = new InvocationTargetException(target);
    Assertions.assertThrows(RetryableException.class, () -> {
      TemporalActivityStubInterceptor.handleInvocationTargetException(e);
    });
  }

}
