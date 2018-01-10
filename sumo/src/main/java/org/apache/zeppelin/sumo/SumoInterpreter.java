package org.apache.zeppelin.sumo;

import java.util.Properties;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterHookRegistry;
import org.apache.zeppelin.interpreter.InterpreterProperty;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterUtils;
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zeppelin.interpreter.Interpreter.FormType;

/**
 * Sumo interpreter for Zeppelin.
 *
 */
public class SumoInterpreter extends Interpreter {
  public static Logger logger = LoggerFactory.getLogger(SumoInterpreter.class);
  private InterpreterOutputStream out;

  public SumoInterpreter(Properties property) {
    super(property);
    out = new InterpreterOutputStream(logger);
  }

  /**
   * Open the interpreter
   */  
  @Override
  public void open() {
    logger.info("Open sumo interpreter");
  }

  /**
   * Optionally implement the canceling routine to abort interpret() method
   */
  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {
    logger.info("Cancelled interpreation");
  }
 
  /**
   * Dynamic form handling
   * see http://zeppelin.apache.org/docs/dynamicform.html
   *
   * @return FormType.SIMPLE enables simple pattern replacement (eg. Hello ${name=world}),
   * FormType.NATIVE handles form in API
   */
  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.NATIVE;
  }
 
  /**
   * Interpret a single line.
   */
  @Override
  public InterpreterResult interpret(String line, InterpreterContext context) {
    logger.info("Interpret a line in sumo");
    System.out.println("SumoInterpreter: " + line);
    return new InterpreterResult(Code.SUCCESS, InterpreterResult.Type.TEXT, "Empty result");
  }

  /**
   * Progress made on interpretation
   *
   * @return number between 0-100
   */
  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 100;
  }


  
  /**
   * Close the interpreter
   */
  @Override
  public void close() {
    logger.info("Close sumo interpreter");
  }
  
}

