
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.kie.api.KieServices;
import org.kie.api.definition.rule.Rule;
import org.kie.api.event.rule.AfterMatchFiredEvent;
import org.kie.api.event.rule.AgendaEventListener;
import org.kie.api.event.rule.AgendaGroupPoppedEvent;
import org.kie.api.event.rule.AgendaGroupPushedEvent;
import org.kie.api.event.rule.BeforeMatchFiredEvent;
import org.kie.api.event.rule.MatchCancelledEvent;
import org.kie.api.event.rule.MatchCreatedEvent;
import org.kie.api.event.rule.RuleFlowGroupActivatedEvent;
import org.kie.api.event.rule.RuleFlowGroupDeactivatedEvent;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.datatorrent.cep.schema.Transaction;
import com.datatorrent.cep.transactionGenerator.TransactionGenerator;

/**
 * Created by ambarish on 25/10/17.
 */
public class LatencyTests
{

  public static void main(String args[])
  {
    Logger.getRootLogger().setLevel(Level.OFF);

    int numTransactions = Integer.parseInt(System.getProperty("n"));
    int numSessions = Integer.parseInt(System.getProperty("s"));
//    int numTransactions = Integer.parseInt("10000");
//    int numSessions = Integer.parseInt("1");

    RuleCountListener ruleCountListener =  new RuleCountListener();
    Runtime runtime = Runtime.getRuntime();
    long beforeUsedMem = runtime.totalMemory() - runtime.freeMemory();
    long startTime = System.nanoTime();
    KieServices kieServices = KieServices.Factory.get();
    KieContainer kieContainer = kieServices.newKieClasspathContainer();
    HashMap<Integer, KieSession> sessions = new HashMap<Integer, KieSession>(numSessions);
    for(int i = 0; i< numSessions; i++){
      final KieSession kieSession = kieContainer.newKieSession();
      kieSession.addEventListener(ruleCountListener);
      sessions.put(i,kieSession);
      new Thread("SessionFireRuleThread-" + i)
      {
        @Override
        public void run()
        {
          kieSession.fireUntilHalt();
        }
      }.start();
    }

    TransactionGenerator gen = getTransactionGenerator();

    try {
      gen.generateData();
    } catch (IOException e) {
      e.printStackTrace();
    }
    KieSession kieSession = null;
    for (int i = 0; i < numTransactions; i++) {
      if(i%1000==0){
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      Transaction t = gen.generateTransaction(null);
        int sid = t.getCustomer().hashCode() % numSessions;

      kieSession = sessions.get(sid);
      kieSession.insert(t);

    }
    long endTime = System.nanoTime();
    double timeSec = (double) (endTime - startTime) / 1000000000L;
    long afterUsedMem = runtime.totalMemory() - runtime.freeMemory();

    try {
      PrintWriter pw = new PrintWriter(new FileOutputStream(new File("log.txt"),true /* append = true */));
      pw.append(numSessions + "," + numTransactions + "," + (double)((double)(afterUsedMem - beforeUsedMem)
        /1000000000) + "," + timeSec + "\n");
      pw.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

//    Map<String,MutableLong> ruleCounts = new HashMap<>();
//    for (int i = 0; i < numSessions; i++){
//      System.out.print("Num Facts : " + sessions.get(i).getFactCount() + " ");
//      for (AgendaEventListener r : sessions.get(i).getAgendaEventListeners()
//      ) {
//          if(r instanceof RuleCountListener){
//            System.out.println(i + " - " + ((RuleCountListener)r).getMatches());
//            System.out.println(((RuleCountListener)r).getRuleCount().toString());
//            Map<String,MutableLong> m = ((RuleCountListener)r).getRuleCount();
//            for (String rule : m.keySet()) {
//                if(ruleCounts.containsKey(rule)){
//                  ruleCounts.get(rule).add(m.get(rule));
//                }else{
//                  ruleCounts.put(rule,m.get(rule));
//                }
//            }
//         }
//      }
//    }

//    System.out.println(ruleCounts.toString());
    System.out.println("Done");
  }


  public static class RuleCountListener implements AgendaEventListener
  {
    long matches;
    Map<String, MutableLong> ruleCount = new HashMap();

    @Override
    public void matchCreated(MatchCreatedEvent matchCreatedEvent)
    {

    }

    @Override
    public void matchCancelled(MatchCancelledEvent matchCancelledEvent)
    {

    }

    @Override
    public void beforeMatchFired(BeforeMatchFiredEvent beforeMatchFiredEvent)
    {
      matches++;
      Rule rule = beforeMatchFiredEvent.getMatch().getRule();
      if (rule != null) {
        String name = rule.getName();
        MutableLong count = ruleCount.get(name);
        if (count == null) {
          count = new MutableLong();
          ruleCount.put(name, count);
        }
        count.increment();
      }
    }

    @Override
    public void afterMatchFired(AfterMatchFiredEvent afterMatchFiredEvent)
    {

    }

    @Override
    public void agendaGroupPopped(AgendaGroupPoppedEvent agendaGroupPoppedEvent)
    {

    }

    @Override
    public void agendaGroupPushed(AgendaGroupPushedEvent agendaGroupPushedEvent)
    {

    }

    @Override
    public void beforeRuleFlowGroupActivated(RuleFlowGroupActivatedEvent ruleFlowGroupActivatedEvent)
    {

    }

    @Override
    public void afterRuleFlowGroupActivated(RuleFlowGroupActivatedEvent ruleFlowGroupActivatedEvent)
    {

    }

    @Override
    public void beforeRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent ruleFlowGroupDeactivatedEvent)
    {

    }

    @Override
    public void afterRuleFlowGroupDeactivated(RuleFlowGroupDeactivatedEvent ruleFlowGroupDeactivatedEvent)
    {

    }

    public long getMatches()
    {
      return matches;
    }

    public Map<String, MutableLong> getRuleCount()
    {
      return ruleCount;
    }
  }

  public static TransactionGenerator getTransactionGenerator()
  {
    TransactionGenerator gen = new TransactionGenerator();
    gen.setFraudTransactionPercentage(5);
    gen.setEnrichCustomers(true);
    gen.setEnrichPaymentCard(true);
    gen.setEnrichProduct(true);
    gen.setEnrichStorePOS(true);
    gen.setNoCards(15000);
    gen.setNoCustomers(10000);
    gen.setNoPOS(5000);
    gen.setNoProducts(20000);
    return gen;
  }

}
