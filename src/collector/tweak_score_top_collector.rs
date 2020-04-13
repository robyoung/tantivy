use crate::collector::top_collector::{TopCollector, TopSegmentCollector};
use crate::collector::{Collector, SegmentCollector};
use crate::DocAddress;
use crate::{DocId, Result, Score, SegmentReader};

pub(crate) struct TweakedScoreTopCollector<TScoreTweaker, TScore = Score> {
    score_tweaker: TScoreTweaker,
    collector: TopCollector<TScore>,
}

impl<TScoreTweaker, TScore> TweakedScoreTopCollector<TScoreTweaker, TScore>
where
    TScore: Clone + PartialOrd,
{
    pub fn new(
        score_tweaker: TScoreTweaker,
        limit: usize,
    ) -> TweakedScoreTopCollector<TScoreTweaker, TScore> {
        TweakedScoreTopCollector {
            score_tweaker,
            collector: TopCollector::with_limit(limit),
        }
    }
}

/// A `ScoreSegmentTweaker` makes it possible to modify the default score
/// for a given document belonging to a specific segment.
///
/// It is the segment local version of the [`ScoreTweaker`](./trait.ScoreTweaker.html).
pub trait ScoreSegmentTweaker<TScore>: 'static {
    /// Tweak the given `score` for the document `doc`.
    fn score(&mut self, doc: DocId, score: Score) -> TScore;
}

/// `ScoreTweaker` makes it possible to tweak the score
/// emitted  by the scorer into another one.
///
/// The `ScoreTweaker` itself does not make much of the computation itself.
/// Instead, it helps constructing `Self::Child` instances that will compute
/// the score at a segment scale.
pub trait ScoreTweaker<TScore>: Sync {
    /// Type of the associated [`ScoreSegmentTweaker`](./trait.ScoreSegmentTweaker.html).
    type Child: ScoreSegmentTweaker<TScore>;

    /// Builds a child tweaker for a specific segment. The child scorer is associated to
    /// a specific segment.
    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child>;
}

impl<TScoreTweaker, TScore> Collector for TweakedScoreTopCollector<TScoreTweaker, TScore>
where
    TScoreTweaker: ScoreTweaker<TScore>,
    TScore: 'static + PartialOrd + Clone + Send + Sync,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    type Child = TopTweakedScoreSegmentCollector<TScoreTweaker::Child, TScore>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let segment_scorer = self.score_tweaker.segment_tweaker(segment_reader)?;
        let segment_collector = self
            .collector
            .for_segment(segment_local_id, segment_reader)?;
        Ok(TopTweakedScoreSegmentCollector {
            segment_collector,
            segment_scorer,
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct TopTweakedScoreSegmentCollector<TSegmentScoreTweaker, TScore>
where
    TScore: 'static + PartialOrd + Clone + Send + Sync + Sized,
    TSegmentScoreTweaker: ScoreSegmentTweaker<TScore>,
{
    segment_collector: TopSegmentCollector<TScore>,
    segment_scorer: TSegmentScoreTweaker,
}

impl<TSegmentScoreTweaker, TScore> SegmentCollector
    for TopTweakedScoreSegmentCollector<TSegmentScoreTweaker, TScore>
where
    TScore: 'static + PartialOrd + Clone + Send + Sync,
    TSegmentScoreTweaker: 'static + ScoreSegmentTweaker<TScore>,
{
    type Fruit = Vec<(TScore, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        let score = self.segment_scorer.score(doc, score);
        self.segment_collector.collect(doc, score);
    }

    fn harvest(self) -> Vec<(TScore, DocAddress)> {
        self.segment_collector.harvest()
    }
}

impl<F, TScore, TSegmentScoreTweaker> ScoreTweaker<TScore> for F
where
    F: 'static + Send + Sync + Fn(&SegmentReader) -> TSegmentScoreTweaker,
    TSegmentScoreTweaker: ScoreSegmentTweaker<TScore>,
{
    type Child = TSegmentScoreTweaker;

    fn segment_tweaker(&self, segment_reader: &SegmentReader) -> Result<Self::Child> {
        Ok((self)(segment_reader))
    }
}

impl<F, TScore> ScoreSegmentTweaker<TScore> for F
where
    F: 'static + Sync + Send + FnMut(DocId, Score) -> TScore,
{
    fn score(&mut self, doc: DocId, score: Score) -> TScore {
        (self)(doc, score)
    }
}

#[cfg(test)]
mod tests {
    use crate::collector::TopDocs;
    use crate::query::BooleanQuery;
    use crate::schema::*;
    use crate::{DocId, Index, Score, SegmentReader};
    use std::collections::HashSet;

    #[test]
    fn tweak_score_with_facets() {
        let mut schema_builder = Schema::builder();

        let title = schema_builder.add_text_field("title", STORED);
        let ingredient = schema_builder.add_facet_field("ingredient");

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema.clone());

        let mut index_writer = index.writer(30_000_000).unwrap();

        index_writer.add_document(doc!(
            title => "Fried egg",
            ingredient => Facet::from("/ingredient/egg"),
            ingredient => Facet::from("/ingredient/oil"),
        ));
        index_writer.add_document(doc!(
            title => "Scrambled egg",
            ingredient => Facet::from("/ingredient/egg"),
            ingredient => Facet::from("/ingredient/butter"),
            ingredient => Facet::from("/ingredient/milk"),
            ingredient => Facet::from("/ingredient/salt"),
        ));
        index_writer.add_document(doc!(
            title => "Egg rolls",
            ingredient => Facet::from("/ingredient/egg"),
            ingredient => Facet::from("/ingredient/garlic"),
            ingredient => Facet::from("/ingredient/salt"),
            ingredient => Facet::from("/ingredient/oil"),
            ingredient => Facet::from("/ingredient/tortilla-wrap"),
            ingredient => Facet::from("/ingredient/mushroom"),
        ));
        index_writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        {
            let facets = vec![
                Facet::from("/ingredient/egg"),
                Facet::from("/ingredient/oil"),
                Facet::from("/ingredient/garlic"),
                Facet::from("/ingredient/mushroom"),
            ];
            let query = BooleanQuery::new_multiterms_query(
                facets
                    .iter()
                    .map(|key| Term::from_facet(ingredient, &key))
                    .collect(),
            );
            let top_docs_by_custom_score =
                TopDocs::with_limit(2).tweak_score(move |segment_reader: &SegmentReader| {
                    let mut ingredient_reader = segment_reader.facet_reader(ingredient).unwrap();
                    let facet_dict = ingredient_reader.facet_dict();

                    let query_ords: HashSet<u64> = facets
                        .iter()
                        .filter_map(|key| facet_dict.term_ord(key.encoded_str()))
                        .collect();

                    let mut facet_ords_buffer: Vec<u64> = Vec::with_capacity(20);

                    move |doc: DocId, original_score: Score| {
                        ingredient_reader.facet_ords(doc, &mut facet_ords_buffer);
                        let missing_ingredients = facet_ords_buffer
                            .iter()
                            .cloned()
                            .collect::<HashSet<u64>>()
                            .difference(&query_ords)
                            .count();
                        let tweak = 1.0 / 4_f32.powi(missing_ingredients as i32);

                        original_score * tweak
                    }
                });
            let top_docs = searcher.search(&query, &top_docs_by_custom_score).unwrap();

            let titles: Vec<String> = top_docs
                .iter()
                .map(|(_, doc_id)| {
                    searcher
                        .doc(*doc_id)
                        .unwrap()
                        .get_first(title)
                        .unwrap()
                        .text()
                        .unwrap()
                        .to_owned()
                })
                .collect();
            assert_eq!(titles, vec!["Fried egg", "Egg rolls"]);
        }
    }
}
