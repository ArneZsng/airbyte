/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.workers.temporal.annotations.TemporalActivityStub;
import io.micronaut.context.BeanRegistration;
import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;
import org.springframework.util.ReflectionUtils;

/**
 * Custom interceptor that handles invocations of Temporal workflow implementations to ensure that
 * any and all Temporal activity stubs are created prior to the first execution of the workflow.
 * This class is used in conjunction with {@link TemporalProxyHelper}.  This approach is inspired by
 * https://github.com/applicaai/spring-boot-starter-temporal.
 *
 * @param <T> The type of the Temporal workflow.
 */
@Slf4j
public class TemporalActivityStubInterceptor<T> {

  /**
   * Function that generates Temporal activity stubs.
   *
   * Replace this value for unit testing.
   */
  private ActivityStubFunction<Class<?>, ActivityOptions, Object> activityStubGenerator = Workflow::newActivityStub;

  /**
   * The collection of configured {@link ActivityOptions} beans provided by the application framework.
   */
  private final Collection<BeanRegistration<ActivityOptions>> activityOptions;

  /**
   * The type of the workflow implementation to be proxied.
   */
  private final Class<T> workflowImplClass;

  /**
   * Constructs a new interceptor for the provided workflow implementation class.
   *
   * @param workflowImplClass The Temporal workflow implementation class that will be intercepted.
   * @param activityOptions The collection of configured {@link ActivityOptions} beans provided by the
   *        application framework.
   */
  public TemporalActivityStubInterceptor(final Class<T> workflowImplClass, final Collection<BeanRegistration<ActivityOptions>> activityOptions) {
    this.workflowImplClass = workflowImplClass;
    this.activityOptions = activityOptions;
  }

  /**
   * Main interceptor method that will be invoked by the proxy.
   *
   * @param workflowImplInstance The actual workflow implementation object invoked on the proxy
   *        Temporal workflow instance.
   * @param call A {@link Callable} used to invoke the proxied method.
   * @return The result of the proxied method execution.
   * @throws Exception if the proxied method throws a checked exception
   * @throws IllegalStateException if the Temporal activity stubs associated with the workflow cannot
   *         be initialized.
   */
  @RuntimeType
  public Object execute(@This final T workflowImplInstance, @SuperCall final Callable<Object> call) throws Exception {
    // Initialize the activity stubs, if not already done, before execution of the workflow method
    initializeActivityStubs(workflowImplClass, workflowImplInstance, activityOptions);
    return call.call();
  }

  /**
   * Initializes all Temporal activity stubs present on the provided workflow instance. A Temporal
   * activity stub is denoted by the use of the {@link TemporalActivityStub} annotation on the field.
   *
   * @param workflowImplClass The target class of the proxy.
   * @param workflowInstance The workflow instance that may contain Temporal activity stub fields.
   * @param activityOptions The collection of {@link ActivityOptions} beans configured in the
   *        application context.
   */
  private void initializeActivityStubs(final Class<T> workflowImplClass,
                                       final T workflowInstance,
                                       final Collection<BeanRegistration<ActivityOptions>> activityOptions) {
    for (final Field field : workflowImplClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(TemporalActivityStub.class)) {
        initializeActivityStub(workflowInstance, field, activityOptions);
      }
    }
  }

  /**
   * Initializes the Temporal activity stub represented by the provided field on the provided object,
   * if not already set.
   *
   * @param workflowInstance The Temporal workflow instance that contains the Temporal activity stub
   *        field.
   * @param activityStubField The field that represents the Temporal activity stub.
   * @param activityOptions The collection of {@link ActivityOptions} beans configured in the
   *        application context.
   */
  private void initializeActivityStub(final T workflowInstance,
                                      final Field activityStubField,
                                      final Collection<BeanRegistration<ActivityOptions>> activityOptions) {
    try {
      log.debug("Attempting to initialize Temporal activity stub for activity '{}' on workflow '{}'...", activityStubField.getType(),
          workflowInstance.getClass().getName());
      ReflectionUtils.makeAccessible(activityStubField);
      if (activityStubField.get(workflowInstance) == null) {
        final Object activityStub = generateActivityStub(activityStubField, getActivityOptions(activityStubField, activityOptions));
        activityStubField.set(workflowInstance, activityStub);
        log.debug("Initialized Temporal activity stub for activity '{}' for workflow '{}'.", activityStubField.getType(),
            workflowInstance.getClass().getName());
      } else {
        log.debug("Temporal activity stub '{}' is already initialized for Temporal workflow '{}'.",
            activityStubField.getType(),
            workflowInstance.getClass().getName());
      }
    } catch (final IllegalArgumentException | IllegalAccessException | IllegalStateException e) {
      log.error("Unable to initialize Temporal activity stub for activity '{}' for workflow '{}'.", activityStubField.getType(),
          workflowInstance.getClass().getName(), e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Extracts the Temporal {@link ActivityOptions} from the {@link Field} on the provided target
   * instance object.
   *
   * @param activityStubField The field that represents the Temporal activity stub.
   * @param activityOptions The collection of {@link ActivityOptions} beans configured in the
   *        application context.
   * @return The Temporal {@link ActivityOptions} from the {@link Field} on the provided Temporal
   *         workflow instance object.
   * @throws IllegalStateException if the referenced Temporal {@link ActivityOptions} bean cannot be
   *         located.
   */
  private ActivityOptions getActivityOptions(final Field activityStubField, final Collection<BeanRegistration<ActivityOptions>> activityOptions) {
    final TemporalActivityStub annotation = activityStubField.getAnnotation(TemporalActivityStub.class);
    final String activityOptionsBeanName = annotation.activityOptionsBeanName();
    final Optional<ActivityOptions> selectedActivityOptions =
        activityOptions.stream().filter(b -> b.getIdentifier().getName().equalsIgnoreCase(activityOptionsBeanName)).map(b -> b.getBean()).findFirst();
    if (selectedActivityOptions.isPresent()) {
      return selectedActivityOptions.get();
    } else {
      throw new IllegalStateException("No activity options bean of name '" + activityOptionsBeanName + "' exists.");
    }
  }

  /**
   * Generates the new activity stub from the workflow.
   *
   * @param activityStubField The field in the workflow implementation that represents the activity
   *        stub.
   * @param activityOptions The {@link ActivityOptions} for the stub.
   * @return The generated activity stub proxy object.
   */
  private Object generateActivityStub(final Field activityStubField, final ActivityOptions activityOptions) {
    return activityStubGenerator.apply(activityStubField.getType(), activityOptions);
  }

  @VisibleForTesting
  void setActivityStubGenerator(final ActivityStubFunction<Class<?>, ActivityOptions, Object> activityStubGenerator) {
    this.activityStubGenerator = activityStubGenerator;
  }

  /**
   * Handle the given {@link InvocationTargetException}. Throws the underlying
   * {@link RuntimeException} or {@link Error} in case of such a root cause. Otherwise, the
   * {@link Exception} is thrown.
   *
   * @param e The {@link InvocationTargetException}.
   * @throws Exception the underlying cause exception extracted from the
   *         {@link InvocationTargetException}.
   */
  @VisibleForTesting
  static void handleInvocationTargetException(final InvocationTargetException e) throws Exception {
    if (e.getTargetException() instanceof Error) {
      throw (Error) e.getTargetException();
    } else if (e.getTargetException() instanceof RuntimeException) {
      throw (RuntimeException) e.getTargetException();
    } else {
      throw (Exception) e.getTargetException();
    }
  }

  /**
   * Functional interface that defines the function used to generate a Temporal activity stub.
   *
   * @param <C> The type of the Temporal activity stub.
   * @param <A> The {@link ActivityOptions} provided to the Temporal activity stub.
   * @param <O> The Temporal activity stub object.
   */
  @FunctionalInterface
  public interface ActivityStubFunction<C extends Class<?>, A extends ActivityOptions, O> {

    O apply(C c, A a);

  }

}
