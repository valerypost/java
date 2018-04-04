package com.adobe.cq.upgrades.cleanup.impl;

import java.util.Arrays;
import java.util.Dictionary;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Session;
import javax.jcr.Workspace;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyUnbounded;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.apache.sling.commons.osgi.PropertiesUtil;
import org.apache.sling.jcr.api.SlingRepository;
import org.apache.sling.jcr.api.SlingRepositoryInitializer;
import org.apache.sling.launchpad.api.StartupHandler;
import org.apache.sling.launchpad.api.StartupMode;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(configurationFactory=true, policy=ConfigurationPolicy.REQUIRE, metatype=true, label="Content Cleanup Component", description="Deletes content while upgrading, during repository initialization before the package manager starts")
@Service({SlingRepositoryInitializer.class})
public class UpgradeContentCleanup
  implements SlingRepositoryInitializer
{
  private final Logger log = LoggerFactory.getLogger(getClass());
  @Reference
  private StartupHandler startupHandler;
  private final AtomicInteger execCounter = new AtomicInteger();
  @Property(unbounded=PropertyUnbounded.ARRAY, label="Snapshot path regular expressions", description="Regular expressions that define which paths are allowed to be deleted")
  public static final String PROP_DELETE_PATH_REGEXPS = "delete.path.regexps";
  private Pattern[] pathPatterns;
  @Property(label="Delete selection query", description="SQL2 query that selects candidates for deletion")
  public static final String PROP_DELETE_QUERY = "delete.sql2.query";
  private String deleteQuery;
  private StartupMode sm = null;
  
  @Activate
  protected void activate(ComponentContext ctx)
    throws InvalidSyntaxException
  {
    this.sm = this.startupHandler.getMode();
    if (this.sm == StartupMode.UPDATE)
    {
      this.log.info("Startup mode is {}, cleanup will be done", this.sm);
    }
    else
    {
      this.log.info("Startup mode is {}, nothing to do", this.sm);
      return;
    }
    this.deleteQuery = PropertiesUtil.toString(ctx.getProperties().get("delete.sql2.query"), null);
    String[] expr = PropertiesUtil.toStringArray(ctx.getProperties().get("delete.path.regexps"), null);
    this.pathPatterns = new Pattern[expr.length];
    for (int i = 0; i < expr.length; i++) {
      this.pathPatterns[i] = Pattern.compile(expr[i]);
    }
    this.log.info("deleteQuery={}", this.deleteQuery);
    this.log.info("deletable paths regexp={}", Arrays.asList(this.pathPatterns));
  }
  
  private void deleteAsynchronously(final String path, final SlingRepository repository)
  {
    this.log.info("Starting background job to delete {}", path);
    Thread t = new Thread("Delete " + path)
    {
      public void run()
      {
        Session s = null;
        try
        {
          s = repository.loginAdministrative(null);
          if (s.nodeExists(path))
          {
            s.getNode(path).remove();
            s.save();
            UpgradeContentCleanup.this.log.info("Deleted {}", path);
          }
          else
          {
            UpgradeContentCleanup.this.log.warn("Path to delete not found: {}", path);
          }
        }
        catch (Exception e)
        {
          UpgradeContentCleanup.this.log.warn("Exception while deleting " + path, e);
        }
        finally
        {
          if (s != null) {
            s.logout();
          }
        }
      }
    };
    t.setDaemon(true);
    t.start();
  }
  
  public void processRepository(SlingRepository repo)
    throws Exception
  {
    if (this.sm == StartupMode.UPDATE) {
      try
      {
        if (this.execCounter.incrementAndGet() != 1)
        {
          this.log.info("Concurrent execution detected, won't do anything");
        }
        else
        {
          Session s = repo.loginAdministrative(null);
          try
          {
            Query q = s.getWorkspace().getQueryManager().createQuery(this.deleteQuery, "JCR-SQL2");
            NodeIterator it = q.execute().getNodes();
            int deleted = 0;
            int ignored = 0;
            String targetFolder = "pre-upgrade-cleanup-" + System.currentTimeMillis();
            String targetPath = s.getNode("/tmp").addNode(targetFolder).getPath();
            s.save();
            while (it.hasNext())
            {
              Node n = it.nextNode();
              if (okToRemove(n.getPath()))
              {
                String moveTo = targetPath + "/" + deleted;
                this.log.info("Moving {} to {}", n.getPath(), moveTo);
                s.getWorkspace().move(n.getPath(), moveTo);
                deleted++;
              }
              else
              {
                ignored++;
              }
            }
            this.log.info("{} nodes deleted, {} ignored", Integer.valueOf(deleted), Integer.valueOf(ignored));
            if (deleted > 0) {
              deleteAsynchronously(targetPath, repo);
            }
          }
          finally
          {
            s.save();
            s.logout();
          }
        }
      }
      catch (Exception e)
      {
        this.log.warn("Exception in doCleanup()", e);
      }
      finally
      {
        this.execCounter.decrementAndGet();
      }
    }
  }
  
  private boolean okToRemove(String path)
  {
    for (Pattern p : this.pathPatterns) {
      if (p.matcher(path).matches())
      {
        this.log.debug("Path {} matches {}, ok to remove", path, p);
        return true;
      }
    }
    this.log.debug("Path {} does not match any pattern, won't remove", path);
    return false;
  }
  
  protected void bindStartupHandler(StartupHandler paramStartupHandler)
  {
    this.startupHandler = paramStartupHandler;
  }
  
  protected void unbindStartupHandler(StartupHandler paramStartupHandler)
  {
    if (this.startupHandler == paramStartupHandler) {
      this.startupHandler = null;
    }
  }
}
