package com.bawei.userdraw

class UserDraw(var mdn: String,var male: Double,var female: Double,var age1: Double,var age2: Double,var age3: Double,var age4: Double,var age5: Double) extends Serializable {

  def this(mdn: String) {
    this(mdn,0.5,0.5,0.2,0.2,0.2,0.2,0.2)
  }

  def protraitSex(male: Double, female: Double, times: Long) = {
    val sum = this.male + this.female + (male + female) * times
    if (sum != 0d) {
      this.male = (this.male + male * times) / sum
      this.female = (this.female + female * times) / sum
    }
  }

  def protraitAge(age1: Double, age2: Double, age3: Double, age4: Double, age5: Double, times: Long): Unit = {
    val sum = this.age1 + this.age2 + this.age3 + this.age4 + this.age5 + (age1 + age2 + age3 + age4 + age5) * times
    if (sum != 0d) {
      this.age1 = (this.age1 + age1 * times) / sum
      this.age2 = (this.age2 + age2 * times) / sum
      this.age3 = (this.age3 + age3 * times) / sum
      this.age4 = (this.age4 + age4 * times) / sum
      this.age5 = (this.age5 + age5 * times) / sum
    }
  }

  override def toString: String = s"${mdn}  ${male}   ${female}  ${age1}   ${age2}   ${age3}   ${age4}   ${age5}"
}
