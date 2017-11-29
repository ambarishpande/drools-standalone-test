package com.github.ambarishpande.droolstest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.definition.KiePackage;
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
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.KnowledgeBase;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderConfiguration;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.kie.internal.definition.KnowledgePackage;
import org.kie.internal.io.ResourceFactory;

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

    int numTransactions = System.getProperty("n") == null ? 0 : Integer.parseInt(System.getProperty("n"));
    int numSessions = System.getProperty("s") == null ? 1 : Integer.parseInt(System.getProperty("s"));
    int tps = System.getProperty("r") == null ? 500 : Integer.parseInt(System.getProperty("r"));
    int interval = System.getProperty("i") == null ? 1000 : Integer.parseInt(System.getProperty("i"));
    boolean fireUntilHalt = System.getProperty("fuh") != null && Boolean.parseBoolean(System.getProperty
      ("fuh"));

    final boolean[] halt = new boolean[numSessions];

    HashMap<Integer, KieSession> sessions = new HashMap<Integer, KieSession>(numSessions);

    Runnable run = null;
    ArrayList<Future> futures = new ArrayList<>(numSessions);
    Runtime runtime = Runtime.getRuntime();
    ExecutorService service = Executors.newFixedThreadPool(numSessions);

    long beforeUsedMem = runtime.totalMemory() - runtime.freeMemory();
    long startTime = System.nanoTime();

    KieContainer kieContainer = initKie(fireUntilHalt);
    Collection<KiePackage> packages = kieContainer.getKieBase().getKiePackages();
    for (KiePackage kiePackage : packages) {
      Collection<Rule> rules = kiePackage.getRules();
      for (Rule rule : rules) {
        System.err.println("Loaded rule: " + rule.getName());
      }
    }

    for (int i = 0; i < numSessions; i++) {
      KieSession kieSession = kieContainer.newKieSession();
      kieSession.addEventListener(new RuleCountListener());
      if (fireUntilHalt) {
        kieSession.setGlobal("output", new Output());
      }
      sessions.put(i, kieSession);
      if (fireUntilHalt) {
        final KieSession kieSession1 = kieSession;
        final int finalI = i;
        run = new Runnable()
        {
          @Override
          public void run()
          {
            kieSession1.fireUntilHalt();
            System.out.println("Halted");
            halt[finalI] = true;
          }
        };
        futures.add(i, service.submit(run));
      }
    }

    TransactionGenerator gen = getTransactionGenerator();
    long count = 0;

    // ingest
    while (true) {

      if (fireUntilHalt) {
        for (int i = 0; i < numSessions; i++) {
          if (halt[i]) {
            futures.get(i).cancel(true);
            futures.set(i, service.submit(run));
            halt[i] = false;
          }
        }
      }

      long startIngest = System.nanoTime();
      for (int i = 0; i < tps; i++) {
        Transaction t = gen.generateTransactionRandom();
        int sid = t.getCustomer().userId.hashCode() % numSessions;
        KieSession kieSession = sessions.get(sid);
        kieSession.insert(t);
        if (!fireUntilHalt) {
          kieSession.fireAllRules();
        }
        count++;
      }

      long timeTaken = System.nanoTime() - startIngest;
      long remainingTime = (1000000000L - timeTaken) / 1000000L;
      if (remainingTime <= 0) {
        System.err.println("Took more time to ingest by " + -remainingTime + " ms");
      } else {
        try {
          Thread.sleep(remainingTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      if ((numTransactions > 0) && (count == numTransactions)) {
        break;
      }
      if (count % interval == 0) {
        System.out.println(count + "," + (double)(runtime.totalMemory() - runtime.freeMemory() - beforeUsedMem)
          / 1000000000L);
      }

    }

    long endTime = System.nanoTime();
    double timeSec = (double)(endTime - startTime) / 1000000000L;
    long afterUsedMem = runtime.totalMemory() - runtime.freeMemory();
    double memoryUsage = ((double)(afterUsedMem - beforeUsedMem) / 1000000000);

    log(numSessions, numTransactions, memoryUsage, timeSec);
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
    gen.setNumCards(15000);
    gen.setNumCustomers(10000);
    gen.setNumStores(5000);
    gen.setNumProducts(20000);
    gen.setMaxCustomers(1000000);
    try {
      gen.generateData();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return gen;
  }

  public static KieContainer initKie(boolean fireUntilHalt)
  {
    KieServices kieServices = KieServices.Factory.get();
    KieContainer kieContainer = kieServices.newKieClasspathContainer();
    KieBase kieBase = kieContainer.getKieBase();

    if (fireUntilHalt) {
      KnowledgeBuilderConfiguration kbuilderConf = KnowledgeBuilderFactory.newKnowledgeBuilderConfiguration();
      KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder(kbuilderConf);
      kbuilder.add(ResourceFactory.newClassPathResource("emitRule.drl"), ResourceType.DRL);
      Collection<KnowledgePackage> knowledgePackages = kbuilder.getKnowledgePackages();
      ((KnowledgeBase)kieBase).addKnowledgePackages(knowledgePackages);
    }
    return kieContainer;
  }

  public static void log(int numSessions, int numTransactions, double memoryUsage, double timeTaken)
  {
    try {
      PrintWriter pw = new PrintWriter(new FileOutputStream(new File("log.txt"), true /* append = true */));
      pw.append(numSessions + "," + numTransactions + "," + memoryUsage + "," + timeTaken + "\n");
      pw.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  public static Map<String, MutableLong> getRuleCounts(HashMap<Integer, KieSession> sessions, int numSessions)
  {
    Map<String, MutableLong> ruleCounts = new HashMap<>();
    for (int i = 0; i < numSessions; i++) {
      for (AgendaEventListener r : sessions.get(i).getAgendaEventListeners()
        ) {
        if (r instanceof RuleCountListener) {
          Map<String, MutableLong> m = ((RuleCountListener)r).getRuleCount();
          for (String rule : m.keySet()) {
            if (ruleCounts.containsKey(rule)) {
              ruleCounts.get(rule).add(m.get(rule));
            } else {
              ruleCounts.put(rule, m.get(rule));
            }
          }
        }
      }
    }
    return ruleCounts;
  }
}
