USENIX Sec '25 Cycle 1 Paper #178 Reviews and Comments
===========================================================================
Paper #178 PICACHV: Formally Verified Enforcement for Data in Use Policies
for Data Analytics


Review #178A
===========================================================================

Paper summary
-------------
This paper presents PICACHV, a system for dynamic information flow checking of
data use policies in databases. The work expresses data use restrictions as
downgrading policies on columns in a database and provides a security monitor
to enforce those policies at execution time. There is a formal semantics for
the query execution and a semantic security guarantee, as well as a mechanized
proof that the security monitor properly enforces the stated guarantee.

Main reasons to accept the paper
--------------------------------
This work pulls ideas from information flow control, a powerful approach to security
that has seen only minimal practical application, and applies it to the highly
practical problem of data use policies in the setting of data analytics.
Specifically, PICACHV focuses on checking that database queries return data
that conforms to given data use policies, and thereby may be processed in
essentially any way after the query returns.

This approach of performing a check at an existing system bottleneck and
requiring that data be allowed to flow in any way after it passes through that
bottleneck is a great idea. It drastically simplifies the structures needed to
enforce security while allowing the solution to plug into any manner of
different systems with different needs, written in different languages.
Leveraging database query planning and execution as that bottleneck is also a
great idea, as the vast majority of sensitive data in a data analytics pipeline
will be coming through a sensitive database.

It is also great to see formal details and theorem statements, showing that the
security is not ad-hoc, but has been thought through. Similarly,
machine-checked proofs substantially increase the assurance that this system
properly ensures the security guarantees claimed in the paper.

Main reasons to reject the paper
--------------------------------
The performance numbers are a major concern. In the example datasets, the
security checks took between 3x and 100x as long as the query execution time.
The paper argues that these numbers "[suggest] potential for future
optimization," but there is no indication of why such an optimization would be
possible.

With these sorts of performance numbers, it is important to understand _why_
things are so slow. There is often a fundamental price to pay for security
enforcement, but it is often nowhere near this large in terms of performance.
Figure 10 shows which operations are causing the bottlenecks, but there is no
indication of why projection is so slow or how more engineering effort could
speed it up significantly. In the absence of such an explanation, it is hard to
believe the claim that these performance overheads could be substantially
reduced simply with engineering effort.

The presentation of the formalism is also somewhat sloppy and difficult to
follow. Several important aspects of the formalism are not clearly defined or
not well explained. For example, there is no intuitive explanation for the
difference between the $\downarrow\downarrow$ rules and the $\Downarrow$ rules.
None of the semantic rules in Figures 6 and 8 are explained in detail, making
it very challenging to follow how they work and evaluate whether or not they
make sense. Those figures also refer to the semantics of $\mathsf{RA}^\mathsf{P}$,
but that language does not appear to be specified anywhere in its entirety.
Similarly, important notation in Figure 7 is left entirely unexplained.

There are also critical concepts that are not well explained. For instance, the
Section 5.2 mentioned that the semantics differ depending on whether or not
there is a group. The exact structure and definition of a group is not
provided. I presume these are groupings, as in grouping query results, but the
paper does not describe how that works specifically in this context, which
seems important (even if perhaps not new).

Figure 5 also raises an important question: is it possible to compose every
pair of policies? The use of a lattice of underlying labels seems to imply that
it should be, but the definitions in Figure 5 are not total. If there is no
flow relationship between $\ell_1$ and $\ell_2$, then none of the rules apply
and there is no combination of the policies. Given how the policies are used,
that seems unfortunate. If there are two policies with incomparable
declassification requirements, it seems that there should be a way to represent
a policy that requires both declassifications, but the way the policy language
is set up, that would require them happening in a specific order, which is
undesirable. Is there really no way to represent that in this language? That
would result in some useful combinations of data being impossible to join
together directly without first cleaning them, which seems less than ideal.

Comments for authors
--------------------
The basic idea behind this paper is really great. Pulling foundational ideas
from IFC literature into data analytics pipelines seems to be a great approach
to solving really important practical security problems. On top of that, this
work builds a strong theoretical model, defines a formal security condition
that mirrors a existing condition in the IFC literature, and provides a
mechanized proof of proper enforcement. That's great! It is more than most
systems even attempt.

Unfortunately, this work is let down by extreme performance overheads without
any real explanation for how they can be removed and sloppy and confusing
descriptions of the formalism.

The discussion section also included the line, "We currently trust the query
planner to produce correct and policy-compliant plans." That was confusing and
concerning. The previous 12 pages had indicated the only the security monitor,
policy declarations, and declassification operations needed to be trusted, but
this indicates that the query planner does as well? What for, and what goes
wrong if it misbehaves?

### Minor concerns

There are also some other much more minor concerns.

The paper presents TEEs very prominently, including the first words of the
abstract and in the opening paragraph of the intro. However, it is not clear
how they are relevant to the work beyond the minor motivation of providing an
environment where a security monitor can be trusted to run properly without
leaking intermediate data. Having such an environment is nice, but a threat
model explicitly "assumes honest but potentially fallible users." In that
setting, one could trust the user to run the monitor and queries properly on
their own machines. Focusing so much on TEEs early on seems to pull away from
the main goals and contributions of the paper.

The security lattice is defined to have 5 labels: $\mathbf{L}$, $\mathbf{N}$,
$\mathbf{A}$, $\mathbf{T}$, and $\mathbf{H}$. Those are motivated as reasonable
options, but then the actual meaning of the policies is defined in terms of the
declassification operations and they are just checked against a sink that
requires fully-public outputs at the end. Is there any real value to having so
many levels in the base lattice and not just $\mathbf{L}$ and $\mathbf{H}$?

Additionally, the declassification policies appear to allow for arbitrary
operations as the declassification requirements. The paper mentions trusting
these functions to behave properly and possibly even allowing user-defined
functions for this purpose. That seems a bit questionable, as it is critical to
the security of the system that these functions actually do what the policies
require of them. It is not clear how to avoid this concern without limiting to
a small number of pre-defined operations (which is also not ideal), but it
would be nice to see some discussion of the security risks of allowing this
more permissive structure.

Figure 8: In the Join rule is very confusing. What is the scope of the $\forall i$?
Is it really mandating that $\mathbf{K}[i] = \langle n_1, n_2 \rangle$ with the
same $n_1$ and $n_2$ for every value of $i$?  Also, $\mathrm{R}$ in the
conclusion appears to not be defined anywhere.

Lastly, there are a few spots where the paper describes certain subjective
conclusions without describing the reasoning or data behind them. For instance,
lines 309-311 say "After surveying a plethora of data use policies in the
market, we have determined that..." but there is no description of what data
use policies were surveyed or how that conclusion is justified. Similarly, the
experimental setup (lines 705-706) say, "we manually crafted policies to
simulate real-world scenarios," but do not mention what these policies are or
how they simulate real-world scenarios. This is important information for
validating the experimental setup and needs to be available somewhere, even if
only in an appendix.

### Nitpicks and Typos

The introduction promises "a rigorous extensional security property." However,
Definition 5.1 is a variant of relaxed noninterference which allows violations
of traditional noninterference, but only if they go through explicit
declassification constructs. That formulation is highly intensional, as it
cares not just about the program inputs and outputs, but the specifics of the
computational process that transformed the inputs into the outputs. The
security property is _semantic_, but not extensional.

The description of policies seems to interchange between $\ell^O$ and
$\ell^\mathit{op}$. It seems from Section 4.2 that $O$ is just a set of
operations, but then shouldn't it be $\ell^{\{\mathit{op}\}}$? This is a little
confusing.

There are several places in the paper where figures are out of order, in the
sense that the figures appear (and are numbered) in one order, and referred to
in another. For example, Figure 11 is referenced significantly before Figure 10.
This can be a bit confusing to follow.

ln 118: "...the existence of such technique*s* and..." (add the "s")

lns 326--327: Definition 4.1 refers to the system as "PICACCHU." Is that an old name that should be updated?

lns 393--399: Here policies refer to $\top$ and $\bot$. Should those be $\mathbf{H}$ and $\mathbf{L}$, respectively?

Figure 6: The premises of FBinary appears to have a few typos. The second
premise has nothing before the $=$ sign (should it be $v$?) and the premise
about applying $f$ has $\llbracket f \rrbracket(v_1)$. Should that be
$\llbracket f \rrbracket(v_1, v_2)$?

ln 728: "Picachv" is written in normal font, not smallcaps.

Recommended decision
--------------------
3. Invite for Major Revision

Confidence in recommended decision
----------------------------------
2. Highly confident (would try to convince others)

Ethics consideration
--------------------
3. No (risks, if any, are appropriately mitigated)

Open science compliance
-----------------------
1. Yes.

Questions for authors' response
-------------------------------
The following questions are in order of importance (most to least).
1. What is causing the slowness in the performance numbers? Are there specific reasons to believe engineering could optimize them away?
2. Is it possible to compose every pair of policies? If so, how?
3. Are TEEs relevant for anything beyond motivation? If so, how?



Review #178B
===========================================================================

Paper summary
-------------
PICACHV enforces data use policies using relational algebra and Trusted Execution Environments (TEEs). It abstracts data operations using relational algebra, expresses hierarchical sensitivity levels and declassification requirements for data, runs across various analytical programs and supports multiple languages. PICACHV checks for policy compliance in real time, and ensures only compliant data is released.

Main reasons to accept the paper
--------------------------------
+ Paper addresses compelling real world security and privacy challenges around data usage and the lack of enforcement mechanisms for data compliance policies that are written in human language.
+ Formal definition of labels for hierarchical, varying sensitivity levels is clever and useful for large data sets and operations.
+ Declassification policy for each data element provides very granular control.

Main reasons to reject the paper
--------------------------------
- It would be helpful to show a few tangible examples of PIKACHV's effectiveness in enforcement of data policies in practice. Good areas of chronic illness and health care but paper was thin on details describing the results of the case studies. 
- Connection to TEE isn't clear. Paper only mentions that this is done in a TEE. Experimental setup doesn't seem to have been done in a TEE?
- How does PIKACHU handle data having multiple declassification policies depending on usage and differing policy of application contexts? For example, usage in medical applications vs finance or others.

Comments for authors
--------------------
+ Information Flow Control is encouraged by NIST SP 800-53 as a well-known method of regulating where and how information can travel within and between systems.
+ TEEs already enforce strict controls over how data flows in and out of enclaves.
+ More nuanced policies might group data elements based on their sensitivity, context, or use case vertical, like finance, medical or education, in which similar data elements might have different policies depending on usage. 
+ How does PIKACHV handle dynamic privacy requirements, such as data policies that may differ based on user roles or organizations?  
+ Can access control policies also be applied at the level of individual cells for additional protection?

Recommended decision
--------------------
2. Accept on Shepherd Approval

Confidence in recommended decision
----------------------------------
4. Not confident (would be convinced by different opinions)

Ethics consideration
--------------------
3. No (risks, if any, are appropriately mitigated)

Open science compliance
-----------------------
1. Yes.

Questions for authors' response
-------------------------------
- The connection to TEE isn't clear. Was the experimental setup done in a TEE? 
- How does PIKACHU handle data having multiple declassification policies depending on the data usage and differing policy of varying application contexts? For example, usage in medical applications vs finance or others. The same data might be handled differently.
- Related to the above question, how does PIKACHV handle dynamic privacy requirements, such as data policies that may differ based on user roles or organizations?
- Can access control policies also be applied at the level of individual cells for additional protection?



Review #178C
===========================================================================

Paper summary
-------------
This paper proposes PICACHV, a runtime monitor for security policies of database queries. The authors define a formal syntax and operational semantics, and prove in Coq that it enforces relaxed noninterference. The authors then implement the runtime monitor and evaluate it against pre-existing datasets.

Main reasons to accept the paper
--------------------------------
- New operational semantics, Coq proof of noninterference for relational algebra
- Runtime monitor artifact seems useful in certain cases

Main reasons to reject the paper
--------------------------------
- The operational semantics rules are not self contained
- The paper does not give evidence that the policy language is expressive enough
- The runtime monitor is underexplained, and carries a large overhead
- Case studies are underexplained
- various inconsistencies in the formal rules, presentation

Comments for authors
--------------------
Thank you for the submission! I can certainly see that runtime enforcement of security policies can be useful for protecting databases from bad queries. The fact that you have formalized your results in Coq is also quite nice.

Unfortunately, I believe the paper is not quite ready for publication. First, there are numerous aspects of the operational semantics / formalization that are not adequately explained, or seem to have typos. For example: 
- What is `update` in Figure 6?
- How are trace elements, such as `TrLinear`, introduced? I only see their definition below 526, and their extraction in Figure 9.
- In the context of the formalization, I have no idea what a "group" is.
- The stepping rules are not explained in the main body. They are very complicated, so should be explained.
- I am confused by the purpose of the stepping rules (e.g., FUnary in Figure 6). As I understand it, these are the stepping rules of the security monitor, so should only admit secure executions. The security-relevant part of FUnary is `p->p'`, where the security policy steps. However, according to Figure 4, the security policy can always step. So how do these rules capture a monitor? Under what circumstances does the monitor block execution?

Next, while the paper uses a previously introduced formalism for downgrade policies, no effort is put forth to show that this formalism, or the label model, is actually useful for data analytics. The only example in the paper is the HIPAA one, but I don't know if this example is representative. There are case studies, but the paper does not discuss the security policies contained within. It would have been nice, as well, to see how you actually encode these security policies in your Rust implementation. 

As a suggestion, I believe that for USENIX, it would be advantageous to only give the most important stepping rules in the main body, and put the full collection in the appendix. It is much better to fully explain a few rules than to include all of them without explanation.

Overall, I believe this paper has good potential, so I definitely encourage the authors to continue working on it!

Recommended decision
--------------------
4. Reject

Confidence in recommended decision
----------------------------------
3. Fairly confident

Ethics consideration
--------------------
3. No (risks, if any, are appropriately mitigated)

Open science compliance
-----------------------
1. Yes.

Questions for authors' response
-------------------------------
- Looking at FUnary, how and where does the runtime monitor enforce that bad queries aren't done?

Required changes or revisions
-----------------------------
- Please explain all stepping rules, or at least the most important ones
- Please ensure the formal definitions are self-contained in the paper
- Give evidence that the policy language is useful for data analytics



Review #178D
===========================================================================

Paper summary
-------------
The paper introduces a security monitor called PICACHV that automatically enforces data use policies in analytics. Policy and relational algebra semantics are formalized in the Coq theorem prover. As a proof of concept, the approach is applied to some real-world use cases.

Main reasons to accept the paper
--------------------------------
+ Interesting and original work

+ Technically rigorous contributions 

+ Good preliminary experimental evaluation

Main reasons to reject the paper
--------------------------------
- The paper needs thorough polishing and rewriting

- Experimental evaluation appears to be quite preliminary and a more thorough evaluation is needed

- Some significant limitations (as openly discussed in the paper)

- Formal proofs not yet disclosed

Comments for authors
--------------------
The presentation of the paper needs some thorough polishing as there are many unclear passages and sentences that require work, e.g. in the abstract "By formalizing policy and relational algebra in Coq", "we prove that PICACHV would perform" (why "would"?), "We integrate PICACHV into existing analytical frameworks, Polars,...".
"We also apply our solution..." Solution is not the right word, approach or method would be better.
Another example of linguistic issues (but there are many more): "This is because of the missing of principled work in this domain" and many other examples.
"It then meticulously examines this plan..." what does "meticulously" mean in this case? Does it explore the whole search space?
"The authors of this paper carefully reviewed related documents and PICACHV. In its design and implementation." and many other sentences requiring work.
A macro is used for "section", but this results in issues like "section 3 provides...", where a sentence beginning with a lowercase letter.

The experimental evaluation is interesting but appears to be quite preliminary, with more thorough experiments needed, including optimizations as stated in the paper.

I particularly appreciated the open and frank discussion of the current limitations. These, however, appear to be quite substantial and one is left to wonder whether more work is needed to tackle at least some of these before publication.

I found it a pity that formal proofs were not yet disclosed as I would have appreciated looking at them.

Recommended decision
--------------------
3. Invite for Major Revision

Confidence in recommended decision
----------------------------------
3. Fairly confident

Ethics consideration
--------------------
3. No (risks, if any, are appropriately mitigated)

Open science compliance
-----------------------
1. Yes.

Questions for authors' response
-------------------------------
The limitations appear quite limiting indeed. How can we be more convinced of the strengths of the current contributions?