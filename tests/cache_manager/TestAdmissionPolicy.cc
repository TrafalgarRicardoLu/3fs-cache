#include <gtest/gtest.h>

#include "cache_manager/admission/AdmissionPolicy.h"
#include "cache_manager/admission/SecondMissAdmissionPolicy.h"
#include "tests/GtestHelpers.h"

namespace hf3fs::cache_manager::test {
namespace {

AdmissionContext miss(uint64_t inode, uint32_t block, uint64_t now) {
  return {{inode, cache::CacheBlockIndex{block}}, now};
}

TEST(TestAdmissionPolicy, AdmitsOnlySecondOrderedMissWithinWindow) {
  SecondMissAdmissionPolicy policy(100, 8);
  EXPECT_EQ(policy.evaluate(miss(1, 0, 10)).action, AdmissionAction::BYPASS);
  EXPECT_EQ(policy.evaluate(miss(1, 0, 10)).action, AdmissionAction::BYPASS);
  EXPECT_EQ(policy.evaluate(miss(1, 0, 9)).action, AdmissionAction::BYPASS);
  EXPECT_EQ(policy.evaluate(miss(1, 0, 11)).action, AdmissionAction::ADMIT);
  EXPECT_EQ(policy.evaluate(miss(1, 0, 12)).action, AdmissionAction::BYPASS);
}

TEST(TestAdmissionPolicy, ExpiryStartsANewWindow) {
  SecondMissAdmissionPolicy policy(10, 8);
  EXPECT_EQ(policy.evaluate(miss(1, 0, 1)).action, AdmissionAction::BYPASS);
  EXPECT_EQ(policy.evaluate(miss(1, 0, 12)).action, AdmissionAction::BYPASS);
  EXPECT_EQ(policy.evaluate(miss(1, 0, 20)).action, AdmissionAction::ADMIT);
}

TEST(TestAdmissionPolicy, EvictsOldestEntryDeterministicallyAtLimit) {
  SecondMissAdmissionPolicy policy(100, 2);
  policy.evaluate(miss(2, 0, 1));
  policy.evaluate(miss(1, 0, 1));
  policy.evaluate(miss(3, 0, 2));
  EXPECT_EQ(policy.size(), 2);
  EXPECT_EQ(policy.evaluate(miss(2, 0, 3)).action, AdmissionAction::ADMIT);
  EXPECT_EQ(policy.evaluate(miss(1, 0, 3)).action, AdmissionAction::BYPASS);
}

TEST(TestAdmissionPolicy, FactoryRejectsUnknownPolicyAndRestartStartsEmpty) {
  ASSERT_ERROR(createAdmissionPolicy("unknown", 10, 1), StatusCode::kInvalidConfig);
  auto first = createAdmissionPolicy("second_miss", 10, 1);
  ASSERT_OK(first);
  EXPECT_EQ((*first)->evaluate(miss(1, 0, 1)).action, AdmissionAction::BYPASS);
  auto restarted = createAdmissionPolicy("second_miss", 10, 1);
  ASSERT_OK(restarted);
  EXPECT_EQ((*restarted)->evaluate(miss(1, 0, 2)).action, AdmissionAction::BYPASS);
}

}  // namespace
}  // namespace hf3fs::cache_manager::test
