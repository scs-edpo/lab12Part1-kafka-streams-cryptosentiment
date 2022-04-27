package magicalpipelines.language;

import magicalpipelines.model.EntitySentiment;
import magicalpipelines.serialization.Tweet;

import java.util.List;

public interface LanguageClient {
  public Tweet translate(Tweet tweet, String targetLanguage);

  public List<EntitySentiment> getEntitySentiment(Tweet tweet);
}
