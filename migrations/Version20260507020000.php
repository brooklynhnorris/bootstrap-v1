<?php

declare(strict_types=1);

namespace DoctrineMigrations;

use Doctrine\DBAL\Schema\Schema;
use Doctrine\Migrations\AbstractMigration;

final class Version20260507020000 extends AbstractMigration
{
    public function getDescription(): string
    {
        return 'Register the 7 foundational content (FC) rules in seo_rules so the V2 promotion path can JOIN to rule metadata.';
    }

    public function up(Schema $schema): void
    {
        $this->addSql(<<<'SQL'
            INSERT INTO seo_rules
              (rule_id, name, category, tier, diagnosis, action_output, assigned, priority, action_family, business_multiplier)
            VALUES
              ('FC-R1',  'Indexed Pages Missing Central Entity Cannot Be Cited',
                 'Entity & Topical Authority', 'A',
                 'Indexed page is missing the central entity. Without a clear primary subject on the page, AI engines cannot extract or cite a coherent answer from this URL.',
                 E'CURRENT STATE
Indexable page lacks a central entity declaration in body content.

YOUR MOVE
- Identify the page''s primary entity (product, service, concept, or person)
- Add an unambiguous central entity statement in the first 100 words of body content
- Reinforce with structured data (Product, Service, Article schema) where applicable',
                 'Brook', 'High', 'content_expand', 1.000),

              ('FC-R3',  'Core Pages Below Minimum Word Count Cannot Establish Authority',
                 'On-Page Content Quality', 'A',
                 'Core page is below minimum word count of 500 words. Pages with insufficient content depth cannot rank competitively or provide complete answers.',
                 E'CURRENT STATE
Core page has fewer than 500 body words.

YOUR MOVE
- Expand body content to at least 500 words (target 800+ for competitive queries)
- Cover supporting subtopics, FAQs, specifications, or related concepts
- Maintain content quality — do not pad with filler',
                 'Brook', 'High', 'content_expand', 1.000),

              ('FC-R5',  'Traffic-Bearing Outer Pages Without Core Links Leak Authority',
                 'Internal Link Architecture', 'A',
                 'Traffic-bearing outer page (>=50 impressions) is missing a link to a core page. Outer pages with traffic are wasted authority unless they funnel users into the core conversion path.',
                 E'CURRENT STATE
Outer page has 50+ impressions but contains no link to a core page.

YOUR MOVE
- Add at least one contextually relevant link from this page to a core (product/service) page
- Use descriptive anchor text matching the target page''s primary query
- Place the link within body content, not just nav/footer',
                 'Brook', 'High', 'link_add', 1.000),

              ('FC-R7',  'Indexed Pages With Missing or Mismatched H1 Lose Query Relevance',
                 'Keyword & Intent Alignment', 'A',
                 'Indexed page H1 is missing or does not match the title tag. Search engines rely on H1-title alignment to confirm primary topic.',
                 E'CURRENT STATE
Indexable page either has no H1 or its H1 does not match the title tag.

YOUR MOVE
- Add a clear, query-aligned H1 if missing
- Align H1 wording with the title tag (same primary entity, same intent)
- Avoid duplicate H1s and keep length under 70 characters',
                 'Brook', 'High', 'metadata_fix', 1.000),

              ('FC-R8',  'Core Pages Without H2 Structure Cannot Be Parsed by AI',
                 'On-Page Content Quality', 'A',
                 'Core page is missing H2 headings. Without H2 sectioning, AI engines cannot extract structured answers and search engines cannot identify topic sections.',
                 E'CURRENT STATE
Core page has zero H2 headings.

YOUR MOVE
- Add at least 2-3 H2 headings that segment the page by sub-topic
- Use query-aligned phrasing in H2s where possible
- Do not use H2s purely for visual styling — they should reflect content structure',
                 'Brook', 'Medium', 'content_expand', 1.000),

              ('FC-R9',  'Core Pages Without Schema Markup Are Invisible to Rich Results',
                 'Schema & Structured Data', 'A',
                 'Core page is missing schema markup. Without structured data, the page cannot earn rich results, AI citations, or knowledge graph eligibility.',
                 E'CURRENT STATE
Core page has no JSON-LD schema markup detected.

YOUR MOVE
- Add appropriate schema for the page type (Product, Service, Article, FAQPage, etc.)
- Validate with Google''s Rich Results Test before deployment
- Reference the rule library for required schema fields per page type',
                 'Brad', 'Medium', 'schema_impl', 1.000),

              ('FC-R10', 'High-Traffic Outer Pages Without Core Links Severely Leak Authority',
                 'Internal Link Architecture', 'A',
                 'High-traffic outer page (>=100 impressions) is missing a link to a core page. Higher-impression variant of FC-R5 with stronger urgency.',
                 E'CURRENT STATE
Outer page has 100+ impressions but contains no link to a core page.

YOUR MOVE
- Add a contextually relevant link from this page to a core page (priority over FC-R5)
- Use descriptive anchor text matching the target page''s primary query
- Audit page for additional internal linking opportunities',
                 'Brook', 'High', 'link_add', 1.000)
            ON CONFLICT (rule_id) DO UPDATE SET
              name           = EXCLUDED.name,
              category       = EXCLUDED.category,
              tier           = EXCLUDED.tier,
              diagnosis      = EXCLUDED.diagnosis,
              action_output  = EXCLUDED.action_output,
              assigned       = EXCLUDED.assigned,
              priority       = EXCLUDED.priority,
              action_family  = EXCLUDED.action_family;
SQL);
    }

    public function down(Schema $schema): void
    {
        $this->addSql("DELETE FROM seo_rules WHERE rule_id IN ('FC-R1','FC-R3','FC-R5','FC-R7','FC-R8','FC-R9','FC-R10')");
    }
}
